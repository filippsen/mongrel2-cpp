#include <string>
#include <sstream>
#include <zmq.hpp>
#include <assert.h>
#include <iostream>
#include "m2pp.hpp"
#include "m2pp_internal.hpp"

namespace m2pp {

connection::connection(const std::string& sender_id_, const std::string& sub_addr_, const std::string& pub_addr_) 
    : sender_id(sender_id_), sub_addr(sub_addr_), pub_addr(pub_addr_) {

    ctx = new zmq::context_t(1);

    reqs = new zmq::socket_t(*ctx, ZMQ_PULL);
    reqs->connect(sub_addr.c_str());

    resp = new zmq::socket_t(*ctx, ZMQ_PUB);
    resp->setsockopt(ZMQ_IDENTITY, sender_id.data(), sender_id.length());
    resp->connect(pub_addr.c_str());
}

connection::~connection() {
    if(resp != NULL)
    {
        resp->close();
        delete resp;
    }
    if(reqs != NULL)
    {
        reqs->close();
        delete reqs;
    }
    if(ctx != NULL)
    {
        delete ctx;
    }
}

/*
 * Updated to recv within try block to enable termination on zmq.
 * On termination the terminated flag is set to true on the returned request object.
 */
request connection::recv() {
    zmq::message_t inmsg;
    try
    {
        reqs->recv(&inmsg);
    }
    catch(const zmq::error_t& t)
    {
        /* context was deleted, so it is time to close socket and exit. */
        reqs->close();
        delete reqs;
        reqs = NULL;
        request req;
        req.terminated = true;
        return req;
    }
    return request::parse(inmsg);
}

/**
 * TERMINATE the sending part of zmq. Call this from your SEND thread.
 *
 * First signal your send thread to stop sending and then to run this function (exit_send())
 * befire iit exits.
 *
 * OR if the sending is done from your main thread, then just call this directly from
 * your main thread.
 *
 **/
void connection::exit_send()
{
    resp->close();
    delete resp;
    resp = NULL;
}

/**
 * TERMINATE the receiving part of zmq. Call this from your MAIN thread.
 *
 * Call this from your main thread to trigger the recv thread (running recv())
 * interrupt its blocking and delete the recv socket.
 *
 * The flag terminated on the request object will be set after returning from recv()
 *
 **/
void connection::exit_recv()
{
    /* Delete the context,
     * this will trigger the thread running the recv function to
     * close and delete the socket from that thread.
     */
    delete ctx;
    ctx = NULL;
}

void connection::reply_http(const request& req, const std::string& response, uint16_t code, const std::string& status, std::vector<header> hdrs) {
    std::ostringstream httpresp;

    httpresp << "HTTP/1.1" << " " << code << " " << status << "\r\n";
    httpresp << "Content-Length: " << response.length() << "\r\n";
    for (std::vector<header>::iterator it=hdrs.begin();it!=hdrs.end();++it) {
        httpresp << it->first << ": " << it->second << "\r\n";
    }
    httpresp << "\r\n" << response;

	reply(req, httpresp.str());
}

void connection::reply(const request& req, const std::string& response) {
    // Using the new mongrel2 format as of v1.3
    std::ostringstream msg;
    msg << req.sender << " " << req.conn_id.size() << ":" << req.conn_id << ", " << response;
    std::string msg_str = msg.str();
    zmq::message_t outmsg(msg_str.length());
    ::memcpy(outmsg.data(), msg_str.data(), msg_str.length());

    if(resp != NULL)
    {
        resp->send(outmsg);
    }
}

void connection::reply_websocket(const request& req, const std::string& response, char opcode, char rsvd) {
    reply(req, utils::websocket_header(response.size(), opcode, rsvd) + response);
}

void connection::deliver(const std::string& uuid, const std::vector<std::string>& idents, const std::string& data) {
    assert(idents.size() <= MAX_IDENTS);
    std::ostringstream msg;
    msg << uuid << " ";

    size_t idents_size(idents.size()-1); // initialize with size needed for spaces
    for (size_t i=0; i<idents.size(); i++) {
        idents_size += idents[i].size();
    }
    msg << idents_size << ":";
    for (size_t i=0; i<idents.size(); i++) {
            msg << idents[i];
        if (i < idents.size()-1)
        msg << " ";
    }
    msg << ", " << data;

    std::string msg_str = msg.str();
    zmq::message_t outmsg(msg_str.length());
    ::memcpy(outmsg.data(), msg_str.data(), msg_str.length());

    if(resp != NULL)
    {
        resp->send(outmsg);
    }
}

void connection::deliver_websocket(const std::string& uuid, const std::vector<std::string>& idents, const std::string& data, char opcode, char rsvd) {
	deliver(uuid, idents, utils::websocket_header(data.size(), opcode, rsvd) + data);
}

request request::parse(zmq::message_t& msg) {
    request req;
    std::string result(static_cast<const char *>(msg.data()), msg.size());

    std::vector<std::string> results = utils::split(result, " ", 3);

    req.sender = results[0];
    req.conn_id = results[1];
    req.path = results[2];

    std::string body;
    std::string ign;

    req.headers = utils::parse_json(utils::parse_netstring(results[3], body));

    req.body = utils::parse_netstring(body, ign);

    //check disconnect flag
    req.disconnect = false;
    for (std::vector<header>::const_iterator it = req.headers.begin();
        it != req.headers.end(); ++it) {
        if (it->first == "METHOD" && it->second == "JSON" && req.body == "{\"type\":\"disconnect\"}") {
            req.disconnect = true;
            break;
        }
    }

    req.terminated = false;

    return req;
}

}

