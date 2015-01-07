import tinyredis.redis; 
import std.stdio;
import std.string;
import std.array;
import core.thread;
import std.array;
import msgpack;
import std.string;

// need to install ssh tunneling to protect redis server

enum MessageType
{
	catalogue,
	search,
	fullcatalogue,
	request,
	status,
}

struct DataRequest
{
	MessageType messageType;
	string[] requestInfo;
}

struct DataServer
{
	string host;
	ushort port;
	Redis redis;

	this(string host,short port)
	{
		this.host=host;
		this.port=port;
		redis=new Redis(host,port);	
	}

	T listen(T)(string channel)
	{
		writefln("*entered listen");
		T dataRequest;
		int i=0;
		while(true)
		{
			auto result=redis.send("BRPOP", channel,0);
			if (result.type!=ResponseType.MultiBulk)
				throw new Exception ("wtf");
			auto msg=cast(ubyte[])(array(result)[1]);
			if (msg.length>3)
			{
				//writefln("msg %s:%s",msg.length,cast(string)(msg));
				dataRequest=msg.unpack!T();
				
				//writefln("%s",dataRequest.messageType);
				//foreach(line;dataRequest.requestInfo)
				//	writefln("%s",line);
				break;
			}

		}
		return dataRequest;
	}

	void flush(string channel)
	{
		int i=0;
		auto result=array(redis.send("BRPOP", channel,0));
		writefln("%s - %s:%s",i,result[0],(cast(string)(result[1])));
		return;
	}

	long send(T)(string channel, T dataRequest)
	{
		writefln("*entered send");
		ubyte[] data=pack(dataRequest);
		//ubyte[]data=cast(ubyte[])"hello there";
		writefln("sent data: %s:%s",data.length,to!string(data));
		auto result=redis.send("RPUSH",channel,cast(char[])(data));
		if (result.type!=ResponseType.Integer)
			throw new Exception("wtf");
		return result.intval;
	}
}	


void main(string[] args)
{
	bool flush=false;
	if (args.length!=2)
	{
		writefln("syntax: redispubsub [client|server]");
		return;
	}

	if (args[1].toLower()=="flush")
		flush=true;
	bool client=(args[1].toLower()=="client");
	auto dataServer=new DataServer("localhost", 6379);
	DataRequest dataRequest;
	writefln("Client==%s",client);
	if (client)
	{
		dataRequest.messageType=MessageType.status;
		dataRequest.requestInfo=["helloo","there"];
		writefln("result: %s",dataServer.send("laeeth2",dataRequest));
		return;
	}
	else if(flush)
	{
		while(true)
		{
			dataServer.flush("laeeth2");
		}
	}

	else {
		while(true)
		{
			auto request=dataServer.listen!DataRequest("laeeth2");
			writefln("Message Type: %s",request.messageType);
			foreach(line;request.requestInfo)
				writefln("request info: %s",line);
		}
		//return 0;
	}
}