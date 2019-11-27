/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <utility>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */


/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        //Add self to the member list
        int id = *(int*)(&memberNode->addr.addr[0]);
        int port = *(short*)(&memberNode->addr.addr[4]);
        ++memberNode->nnb;
        MemberListEntry memberListEntry = MemberListEntry(id, port, memberNode->heartbeat, par->getcurrtime());
        memberNode->memberList.push_back(memberListEntry);
        addressMap.insert(std::make_pair(std::to_string(id) + ":" + std::to_string(port),memberListEntry));
        log->logNodeAdd(&memberNode->addr, &memberNode->addr);
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    MessageHdr *pMessageHdr= (MessageHdr *) data;
    MsgTypes type = pMessageHdr->msgType;
    char * content = (char *)(pMessageHdr+1);
    switch (type) {
        case JOINREQ:
           handleMemberJoinRequest(pMessageHdr);
            break;
        case JOINREP:
            memberNode->inGroup = true;
            updateMembers(content);
            break;
        case HEARTBEAT:
            updateMembers(content);
            break;
        default:
            return true;
    }
    return false;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    memberNode->heartbeat += 1;
    long currentTime = par->getcurrtime();
    MemberListEntry * currentNodeEntry = nullptr;
    for (MemberListEntry & entry : memberNode->memberList) {
        Address address = Address(std::to_string(entry.id) + ":" + std::to_string(entry.port));
        if (address == memberNode->addr) {
            currentNodeEntry = &entry;
            break;
        }
    }

    currentNodeEntry->timestamp = currentTime;
    currentNodeEntry->heartbeat = memberNode->heartbeat;

    //Remove stale member
    vector<MemberListEntry>::iterator iterator;
    for (iterator = memberNode->memberList.begin(); iterator != memberNode->memberList.end(); ++iterator) {
        if (currentTime >= iterator->timestamp  + TCLEANUP) {
            Address address = Address(std::to_string(iterator->id) + ":" + std::to_string(iterator->port));
            log->logNodeRemove(&memberNode->addr, &address);
            memberNode->memberList.erase(iterator);
        }
    }

    std::pair<MessageHdr *, size_t> pair = serializeMemberList();
    MessageHdr * messageHdr = pair.first;
    messageHdr->msgType = HEARTBEAT;
    size_t messageSize = pair.second;

    for (MemberListEntry entry : memberNode->memberList) {
        if (currentTime - entry.timestamp >= TFAIL) {
            continue;
        }
        Address address = Address(std::to_string(entry.id) + ":" + std::to_string(entry.port));
        emulNet->ENsend(&memberNode->addr, &address, (char *)messageHdr, messageSize);
    }
    free(messageHdr);
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}


void MP1Node::updateMember(Address memberAddr, long heartbeat) {
	int id = *(int*)(&memberAddr.addr[0]);
	int port = *(short*)(&memberAddr.addr[4]);
    bool isNewMember = true;
    for (MemberListEntry & entry : memberNode->memberList) {
        if (entry.id == id && entry.port == port) {
            isNewMember = false;
            if (entry.heartbeat < heartbeat){
                entry.heartbeat = heartbeat;
                entry.timestamp = par->getcurrtime();
            }
            break;
        }
    }

    if (isNewMember) {
        MemberListEntry memberListEntry = MemberListEntry(id, port, heartbeat, par->getcurrtime());
        memberNode->memberList.push_back(memberListEntry);
        ++memberNode->nnb;
        addressMap.insert(std::make_pair(std::to_string(id) + ":" + std::to_string(port),memberListEntry));
        log->logNodeAdd(&memberNode->addr, &memberAddr);
    }
}



std::pair<MessageHdr *, size_t> MP1Node::serializeMemberList() {
    int size = memberNode->memberList.size();
    size_t msgsize = sizeof(MessageHdr) + sizeof(long) + size * sizeof(MemberListEntry);
    MessageHdr *messageHdr = (MessageHdr *) malloc(msgsize * sizeof(char));

    //Assign temporary message type for now.
    //Serialization format is: message type, size of member list, the elements of list (member entry)
    messageHdr->msgType = UNKNOWN;
    memcpy((char *)(messageHdr+1), &size, sizeof(int));
    char *pContent = (char *)(messageHdr+1) + sizeof(int);
    vector<MemberListEntry>::iterator iterator;
    for (iterator = memberNode->memberList.begin(); iterator != memberNode->memberList.end(); ++iterator) {
        memcpy(pContent, &(*iterator), sizeof(MemberListEntry));
        pContent += sizeof(MemberListEntry);
    }
    return std::make_pair(messageHdr, msgsize);
}

void MP1Node::handleMemberJoinRequest(MessageHdr * msg) {
    Address fromAddress;
    long fromHeartBeat;
    //Deserialize join request
    memcpy(&fromAddress, (char *)(msg+1), sizeof(memberNode->addr.addr));
    memcpy(&fromHeartBeat, (char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), sizeof(long));
    updateMember(fromAddress, fromHeartBeat);

    //Reply join request
    std::pair<MessageHdr *, size_t> pair = serializeMemberList();
    MessageHdr * messageHdr = pair.first;
    messageHdr->msgType = JOINREP;
    size_t messageSize = pair.second;
    emulNet->ENsend(&memberNode->addr, &fromAddress, (char *)messageHdr, messageSize);
    free(messageHdr);
}

void MP1Node::updateMembers(char *content) {
    int incomingDataSize;
    //Deserialize entry count
    memcpy(&incomingDataSize, (char *)content, sizeof(int));
    MemberListEntry* memberListEntries = (MemberListEntry*) (content + sizeof(int));
    for (int i = 0; i < incomingDataSize; ++i) {
        Address address = Address(std::to_string(memberListEntries[i].id) + ":" + std::to_string(memberListEntries[i].port));
        long heartbeat = memberListEntries[i].heartbeat;
        int id = memberListEntries[i].id;
        int port = memberListEntries[i].port;
        bool isNewMember = true;
        //Find
        for (MemberListEntry & entry : memberNode->memberList) {
            if (entry.id == id && entry.port == port) {
                isNewMember = false;
                if (entry.heartbeat < heartbeat){
                    entry.heartbeat = heartbeat;
                    entry.timestamp = par->getcurrtime();
                }
                break;
            }
        }
        //Insert
        if (isNewMember) {
            MemberListEntry memberListEntry = MemberListEntry(id, port, heartbeat, par->getcurrtime());
            memberNode->memberList.push_back(memberListEntry);
            ++memberNode->nnb;
            addressMap.insert(std::make_pair(std::to_string(id) + ":" + std::to_string(port),memberListEntry));
            log->logNodeAdd(&memberNode->addr, &address);
        }
    }
}
