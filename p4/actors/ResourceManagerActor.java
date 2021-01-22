package cmsc433.p4.actors;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import cmsc433.p4.enums.*;
import cmsc433.p4.messages.*;
import cmsc433.p4.util.*;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.AbstractActor.Receive;
import akka.actor.AbstractActor;

public class ResourceManagerActor extends AbstractActor {

	private ActorRef logger;					// Actor to send logging messages to
	private HashMap<String,ActorRef> srcToManager;
	private ArrayList<ActorRef> managers;
	private HashMap<String,Resource> localResources;
	private ArrayList<ActorRef> localUsers;
	private HashMap<String,ArrayList<ManagementRequestMsg>> pendingDisabled;
	private HashMap<String,LinkedBlockingQueue<Object>> blockingRequests;
	private HashMap<String,ArrayList<ActorRef>> writeAccess;
	private HashMap<String,LinkedBlockingQueue<ActorRef>> readAccess;
	private HashMap<String,Integer> srcSearch;
	private HashMap<String,ArrayList<Object>> waitForSearch;

	/**
	 * Props structure-generator for this class.
	 * @return  Props structure
	 */
	static Props props (ActorRef logger) {
		return Props.create(ResourceManagerActor.class, logger);
	}

	/**
	 * Factory method for creating resource managers
	 * @param logger			Actor to send logging messages to
	 * @param system			Actor system in which manager will execute
	 * @return					Reference to new manager
	 */
	public static ActorRef makeResourceManager (ActorRef logger, ActorSystem system) {
		ActorRef newManager = system.actorOf(props(logger));
		return newManager;
	}

	/**
	 * Sends a message to the Logger Actor
	 * @param msg The message to be sent to the logger
	 */
	public void log (LogMsg msg) {
		logger.tell(msg, getSelf());
	}

	/**
	 * Constructor
	 * 
	 * @param logger			Actor to send logging messages to
	 */
	private ResourceManagerActor(ActorRef logger) {
		super();
		this.logger = logger;
		this.localResources = new HashMap<String,Resource>();
		this.srcToManager = new HashMap<String,ActorRef>();
		this.blockingRequests = new HashMap<String,LinkedBlockingQueue<Object>>();
		this.writeAccess = new HashMap<String,ArrayList<ActorRef>>();
		this.readAccess = new HashMap<String,LinkedBlockingQueue<ActorRef>>();
		this.waitForSearch = new HashMap<String,ArrayList<Object>>(); 
		this.srcSearch = new HashMap<String,Integer>();
		this.pendingDisabled = new HashMap<String,ArrayList<ManagementRequestMsg>>();
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(Object.class, this::onReceive)
				.build();
	}


	public void checkDisabled(String src) {
		// if no disable requests then do nothing
		if(!pendingDisabled.get(src).isEmpty() && localResources.get(src).getStatus() != ResourceStatus.DISABLED) {
			// there is atleast 1 disable request so check if it can be granted
			if(readAccess.get(src).isEmpty() && writeAccess.get(src).isEmpty()) {
				// no write or read keys outstanding disable and send the success messages
				localResources.get(src).disable();
				log(LogMsg.makeResourceStatusChangedLogMsg(getSelf(), src, ResourceStatus.DISABLED));
				for(ManagementRequestMsg m : pendingDisabled.get(src)) {
					log(LogMsg.makeManagementRequestGrantedLogMsg(m.getReplyTo(), getSelf(),m.getRequest()));
					ManagementRequestGrantedMsg granted = new ManagementRequestGrantedMsg(m);
					m.getReplyTo().tell(granted, getSelf());
				}
				pendingDisabled.get(src).clear();
			}

		}
	}

	// process all the requests in the blocking queue and remove the ones that are addressed
	// at end check if resource should be disabled and respond to disable requests appropriately
	public void processBlocking(String src) {
		// this might not be in order
		LinkedBlockingQueue<Object> toRemove = new LinkedBlockingQueue<Object>(blockingRequests.get(src));
		for(Object o : toRemove ) {
			if(o instanceof AccessRequestMsg ) {
				AccessRequestMsg temp = (AccessRequestMsg) o;
				// TODO: fix this condition so that it checks that the list is size 0 not boolean anymore
				//might need to add a second condition checking the resources actual disable status
				if(pendingDisabled.get(src).isEmpty() && localResources.get(src).getStatus() != ResourceStatus.DISABLED) {
					if(temp.getAccessRequest().getType() == AccessRequestType.CONCURRENT_READ_BLOCKING) {
						// concurrent read: check that no exclusive write access exists or that its owned by this user
						if(writeAccess.get(src).size() == 0 || writeAccess.get(src).get(0) == temp.getReplyTo()) {
							readAccess.get(src).add(temp.getReplyTo());
							// Access Request Granted Log
							log(LogMsg.makeAccessRequestGrantedLogMsg (temp.getReplyTo(),getSelf(),temp.getAccessRequest()));
							blockingRequests.get(src).remove(o);
							AccessRequestGrantedMsg granted = new AccessRequestGrantedMsg(temp);
							temp.getReplyTo().tell(granted, getSelf());
						}

					}
					else {
						// exclusive write
						if((readAccess.get(src).size() == 0 || onlyContains(temp.getReplyTo(),src)) && (writeAccess.get(src).size() == 0 || writeAccess.get(src).get(0) == temp.getReplyTo())) {
							writeAccess.get(src).add(temp.getReplyTo());
							// Access Request Granted Log
							log(LogMsg.makeAccessRequestGrantedLogMsg (temp.getReplyTo(),getSelf(),temp.getAccessRequest()));
							blockingRequests.get(src).remove(o);
							AccessRequestGrantedMsg granted = new AccessRequestGrantedMsg(temp);
							temp.getReplyTo().tell(granted, getSelf());

						}

					}
				}
				else {
					// access request denied due to disable
					AccessRequestDeniedMsg denied = new AccessRequestDeniedMsg(temp,AccessRequestDenialReason.RESOURCE_DISABLED);
					log(LogMsg.makeAccessRequestDeniedLogMsg(temp.getReplyTo(), getSelf(),temp.getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
					blockingRequests.get(src).remove(o);
					temp.getReplyTo().tell(denied, getSelf());
				}
			}
		}
	}

	// my own method
	// TODO: double check this you mightve goofed
	public ActorRef getManager(String srcName,Object request) {
		// check if local
		if(localResources.containsKey(srcName)) {
			return getSelf();
		}
		else {
			// search for a manager who has the resource
			if(srcToManager.containsKey(srcName)) {
				return srcToManager.get(srcName);
			}
			else {
				// send a whoHasResourceRequestMsg to every manager to see if the resource exists
				// add the src to a hashmap to keep track of how many responses you get (in order to tell if it exists)
				if(!srcSearch.containsKey(srcName)) {
					srcSearch.put(srcName, 0);
					waitForSearch.put(srcName, new ArrayList<Object>());
				}
				// add this request to a list of requests pending on the search
				waitForSearch.get(srcName).add(request);
				for(ActorRef m : managers) {
					// check to make sure you dont send the request to yourself
					if(m != getSelf()) {
						WhoHasResourceRequestMsg msg = new WhoHasResourceRequestMsg(srcName);
						m.tell(msg, getSelf());
					}

				}
				return null;
			}
		}
	}



	public Boolean onlyContains(ActorRef user, String src) {
		Boolean flag = true;
		for(ActorRef r : readAccess.get(src)) {
			if(r != user) {
				flag = false;
			}
		}
		return flag;
	}

	// if manager is null then send denial response to the sender of the msg 
	// else send the msg to the manager for them to handle
	// msg can only be a few things so convert appropriately
	public void handleWhoHas(Object msg, ActorRef manager) {
		Object response = null;
		ActorRef user = null;
		LogMsg logger = null;
		LogMsg forward = null;
		if(msg instanceof AccessRequestMsg) {
			AccessRequestMsg temp = (AccessRequestMsg) msg;
			response = new AccessRequestDeniedMsg(((AccessRequestMsg) msg),AccessRequestDenialReason.RESOURCE_NOT_FOUND);
			user = ((AccessRequestMsg) msg).getReplyTo();
			logger = LogMsg.makeAccessRequestDeniedLogMsg(user, getSelf(), temp.getAccessRequest(),AccessRequestDenialReason.RESOURCE_NOT_FOUND);
			forward = LogMsg.makeAccessRequestForwardedLogMsg(getSelf(), manager, temp.getAccessRequest());
		}
		else if(msg instanceof AccessReleaseMsg) {
			user = null;
			forward = LogMsg.makeAccessReleaseForwardedLogMsg(getSelf(), manager, ((AccessReleaseMsg)msg).getAccessRelease());
		}
		else if(msg instanceof ManagementRequestMsg) {
			ManagementRequestMsg temp = (ManagementRequestMsg) msg;
			response = new ManagementRequestDeniedMsg((ManagementRequestMsg) msg,ManagementRequestDenialReason.RESOURCE_NOT_FOUND);
			user = ((ManagementRequestMsg) msg).getReplyTo();
			logger = LogMsg.makeManagementRequestDeniedLogMsg(user, getSelf(), temp.getRequest(), ManagementRequestDenialReason.RESOURCE_NOT_FOUND);
			forward = LogMsg.makeManagementRequestForwardedLogMsg(getSelf(), manager,temp.getRequest());
		}
		if(user != null) {
			if(manager == null) {
				log(logger);
				user.tell(response, getSelf());
				// log denial
			}
			else {
				// log forwarding
				log(forward);
				manager.tell(msg, user);
			}
		}
	}




	// You may want to add data structures for managing local resources and users, storing
	// remote managers, etc.
	//
	// REMEMBER:  YOU ARE NOT ALLOWED TO CREATE MUTABLE DATA STRUCTURES THAT ARE SHARED BY
	// MULTIPLE ACTORS!

	/* (non-Javadoc)
	 * 
	 * You must provide an implementation of the onReceive() method below.
	 * 
	 * @see akka.actor.AbstractActor#createReceive
	 */

	public void onReceive(Object msg) throws Exception {	

		if(msg instanceof AddInitialLocalResourcesRequestMsg) {
			AddInitialLocalResourcesRequestMsg temp = (AddInitialLocalResourcesRequestMsg) msg;
			String name = "";
			for(Resource r : temp.getLocalResources()) {
				// log every resource that is added to this manager
				log(LogMsg.makeLocalResourceCreatedLogMsg(getSelf(), r.getName()));
				r.enable();
				log(LogMsg.makeResourceStatusChangedLogMsg(getSelf(), r.getName(), ResourceStatus.ENABLED));
				name = r.getName();
				this.readAccess.put(name, new LinkedBlockingQueue<ActorRef>());
				this.blockingRequests.put(name, new LinkedBlockingQueue<Object>());
				this.pendingDisabled.put(name, new ArrayList<ManagementRequestMsg>());
				this.writeAccess.put(name, new ArrayList<ActorRef>());
				// add every resource to the list
				this.localResources.put(r.getName(),r);
			}
			AddInitialLocalResourcesResponseMsg response = new AddInitialLocalResourcesResponseMsg(temp);
			getSender().tell(response, getSelf());
		}
		else if(msg instanceof AddLocalUsersRequestMsg) {
			AddLocalUsersRequestMsg temp = (AddLocalUsersRequestMsg)msg;
			this.localUsers = temp.getLocalUsers();
			AddLocalUsersResponseMsg response = new AddLocalUsersResponseMsg(temp);
			getSender().tell(response, getSelf());
		}
		// this messages list might contain this manager itself so watch out
		else if(msg instanceof AddRemoteManagersRequestMsg) {
			AddRemoteManagersRequestMsg temp = (AddRemoteManagersRequestMsg)msg;
			this.managers = temp.getManagerList();
			AddRemoteManagersResponseMsg response = new AddRemoteManagersResponseMsg(temp);
			getSender().tell(response, getSelf());
		}
		else if(msg instanceof AccessRequestMsg) {
			AccessRequestMsg temp = (AccessRequestMsg) msg;
			log(LogMsg.makeAccessRequestReceivedLogMsg (temp.getReplyTo(), getSelf(), temp.getAccessRequest()));
			ActorRef man = getManager(temp.getAccessRequest().getResourceName(),msg);
			// log access Request received
			if(man != null) {
				if(man == getSelf()) {
					// the resource is local so handle it
					AccessRequest req = temp.getAccessRequest();
					String src = req.getResourceName();
					switch(req.getType()) {
					case CONCURRENT_READ_BLOCKING:
						// check that the resource isnt pending disabled, and the resource either does not have any write accesses or the access is owned by the requestor
						if(pendingDisabled.get(src).isEmpty() && localResources.get(src).getStatus() != ResourceStatus.DISABLED) {
							if(writeAccess.get(src).size() == 0 || writeAccess.get(src).get(0) == temp.getReplyTo()) {
								readAccess.get(src).add(temp.getReplyTo());
								// Access Request Granted Log and message
								log(LogMsg.makeAccessRequestGrantedLogMsg (temp.getReplyTo(),getSelf(),temp.getAccessRequest()));
								AccessRequestGrantedMsg granted = new AccessRequestGrantedMsg(temp);
								temp.getReplyTo().tell(granted, getSelf());
							}
							else {
								// instead of denying when busy we will add the request to the blocking list for this resource
								blockingRequests.get(src).put(msg);
							}
						}
						else {
							AccessRequestDeniedMsg denied = new AccessRequestDeniedMsg(temp,AccessRequestDenialReason.RESOURCE_DISABLED);
							log(LogMsg.makeAccessRequestDeniedLogMsg(temp.getReplyTo(), getSelf(),temp.getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
							getSender().tell(denied, getSelf());
						}
						break;
					case CONCURRENT_READ_NONBLOCKING:
						// check that the resource isnt pending disabled, and the resource either does not have any write accesses or the access is owned by the requestor
						if(pendingDisabled.get(src).isEmpty() && localResources.get(src).getStatus() != ResourceStatus.DISABLED) {
							if(writeAccess.get(src).size() == 0 || writeAccess.get(src).get(0) == temp.getReplyTo()) {
								readAccess.get(src).add(temp.getReplyTo());
								// Access Request Granted Log
								log(LogMsg.makeAccessRequestGrantedLogMsg (temp.getReplyTo(),getSelf(),temp.getAccessRequest()));
								AccessRequestGrantedMsg granted = new AccessRequestGrantedMsg(temp);
								temp.getReplyTo().tell(granted, getSelf());
							}
							else {
								AccessRequestDeniedMsg denied = new AccessRequestDeniedMsg(temp,AccessRequestDenialReason.RESOURCE_BUSY);
								log(LogMsg.makeAccessRequestDeniedLogMsg(temp.getReplyTo(), getSelf(),temp.getAccessRequest(), AccessRequestDenialReason.RESOURCE_BUSY));
								getSender().tell(denied, getSelf());
							}
						}
						else {
							AccessRequestDeniedMsg denied = new AccessRequestDeniedMsg(temp,AccessRequestDenialReason.RESOURCE_DISABLED);
							log(LogMsg.makeAccessRequestDeniedLogMsg(temp.getReplyTo(), getSelf(),temp.getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
							getSender().tell(denied, getSelf());
						}
						break;
					case EXCLUSIVE_WRITE_BLOCKING:
						// check that the resource isnt pending disabled, and the resource either does not have any write accesses or the access is owned by the requestor
						if(pendingDisabled.get(src).isEmpty() && localResources.get(src).getStatus() != ResourceStatus.DISABLED) {
							// also need to check that there are currently no read access keys held by someone other than the requester
							if((readAccess.get(src).size() == 0 || onlyContains(temp.getReplyTo(),src)) && (writeAccess.get(src).size() == 0 || writeAccess.get(src).get(0) == temp.getReplyTo())) {
								writeAccess.get(src).add(temp.getReplyTo());
								// Access Request Granted Log
								log(LogMsg.makeAccessRequestGrantedLogMsg (temp.getReplyTo(),getSelf(),temp.getAccessRequest()));
								AccessRequestGrantedMsg granted = new AccessRequestGrantedMsg(temp);
								temp.getReplyTo().tell(granted, getSelf());
							}
							else {
								// instead of denying when busy we will add the request to the blocking list for this resource
								blockingRequests.get(src).put(msg);
							}
						}
						else {
							AccessRequestDeniedMsg denied = new AccessRequestDeniedMsg(temp,AccessRequestDenialReason.RESOURCE_DISABLED);
							log(LogMsg.makeAccessRequestDeniedLogMsg(temp.getReplyTo(), getSelf(),temp.getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
							getSender().tell(denied, getSelf());
						}
						break;
					case EXCLUSIVE_WRITE_NONBLOCKING:
						// check that the resource isnt pending disabled, and the resource either does not have any write accesses or the access is owned by the requestor
						if(pendingDisabled.get(src).isEmpty() && localResources.get(src).getStatus() != ResourceStatus.DISABLED) {
							// also need to check that there are currently no read access keys held by someone other than the requester
							if((readAccess.get(src).size() == 0 || onlyContains(temp.getReplyTo(),src)) && (writeAccess.get(src).size() == 0 || writeAccess.get(src).get(0) == temp.getReplyTo())) {
								writeAccess.get(src).add(temp.getReplyTo());
								// Access Request Granted Log
								log(LogMsg.makeAccessRequestGrantedLogMsg (temp.getReplyTo(),getSelf(),temp.getAccessRequest()));
								AccessRequestGrantedMsg granted = new AccessRequestGrantedMsg(temp);
								temp.getReplyTo().tell(granted, getSelf());
							}
							else {
								AccessRequestDeniedMsg denied = new AccessRequestDeniedMsg(temp,AccessRequestDenialReason.RESOURCE_BUSY);
								// instead of denying when busy we will add the request to the blocking list for this resource
								log(LogMsg.makeAccessRequestDeniedLogMsg(temp.getReplyTo(), getSelf(),temp.getAccessRequest(), AccessRequestDenialReason.RESOURCE_BUSY));
								getSender().tell(denied, getSelf());
							}
						}
						else {
							AccessRequestDeniedMsg denied = new AccessRequestDeniedMsg(temp,AccessRequestDenialReason.RESOURCE_DISABLED);
							log(LogMsg.makeAccessRequestDeniedLogMsg(temp.getReplyTo(), getSelf(),temp.getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
							getSender().tell(denied, getSelf());
						}
						break;
					}

				}
				else {
					// manager of the resource is someone else
					// TODO: might want to change getSender to temp.getReply()
					log(LogMsg.makeAccessRequestForwardedLogMsg(getSelf(), man, temp.getAccessRequest()));
					man.tell(msg, getSender());
				}
			}
		}
		else if(msg instanceof AccessReleaseMsg) {
			AccessReleaseMsg temp = (AccessReleaseMsg) msg;
			String srcName = temp.getAccessRelease().getResourceName();
			log(LogMsg.makeAccessReleaseReceivedLogMsg(temp.getSender(), getSelf(), temp.getAccessRelease()));
			ActorRef man = getManager(temp.getAccessRelease().getResourceName(),msg);
			// log access Release received

			if(man != null) {
				// if resource local
				if(man == getSelf()) {
					switch(((AccessReleaseMsg) msg).getAccessRelease().getType()) {
					case CONCURRENT_READ:
						// if the requester has access remove it else ignore
						if(readAccess.get(srcName).contains(temp.getSender())) {
							readAccess.get(srcName).remove(temp.getSender());
							// process blocking requests
							processBlocking(srcName);
							checkDisabled(srcName);
						}
						// TODO: add access release ignored
						break;
					case EXCLUSIVE_WRITE:
						if(writeAccess.get(srcName).contains(temp.getSender())) {
							writeAccess.get(srcName).remove(temp.getSender());
							// process blocking requests
							processBlocking(srcName);
							checkDisabled(srcName);
						}
						break;
					}

				}
				else {
					// resource isnt local
					log(LogMsg.makeAccessReleaseForwardedLogMsg(getSelf(), man, temp.getAccessRelease()));
					man.tell(msg, getSender());
				}

			}

		}
		else if(msg instanceof ManagementRequestMsg) {
			ManagementRequestMsg temp = (ManagementRequestMsg) msg;
			String srcName = temp.getRequest().getResourceName();
			log(LogMsg.makeManagementRequestReceivedLogMsg(temp.getReplyTo(), getSelf(),temp.getRequest()));
			ActorRef man = getManager(temp.getRequest().getResourceName(),msg);
			if(man != null) {
				if(man == getSelf()) {
					if(temp.getRequest().getType() == ManagementRequestType.DISABLE) {
						// disable: add to pendingDisable and run checkDisabled if the requester can disable
						// to check if they can disable check to make sure they dont currently hold any access keys
						if(readAccess.get(srcName).contains(temp.getReplyTo()) || writeAccess.get(srcName).contains(temp.getReplyTo())) {
							// requester holds access so deny
							log(LogMsg.makeManagementRequestDeniedLogMsg(temp.getReplyTo(), getSelf(), temp.getRequest(), ManagementRequestDenialReason.ACCESS_HELD_BY_USER));
							ManagementRequestDeniedMsg denied = new ManagementRequestDeniedMsg(temp, ManagementRequestDenialReason.ACCESS_HELD_BY_USER);
							temp.getReplyTo().tell(denied, getSelf());
						}
						else {
							pendingDisabled.get(srcName).add(temp);
							processBlocking(srcName);
							checkDisabled(srcName);
						}

					}
					else {
						// enable : enable the resource and clear the disable requests
						// TODO: add resource changed log
						
						if(localResources.get(srcName).getStatus() == ResourceStatus.DISABLED) {
							localResources.get(srcName).enable();
							log(LogMsg.makeResourceStatusChangedLogMsg(getSelf(), srcName, ResourceStatus.ENABLED));
						}
						log(LogMsg.makeManagementRequestGrantedLogMsg(temp.getReplyTo(), getSelf(), temp.getRequest()));
						ManagementRequestGrantedMsg granted = new ManagementRequestGrantedMsg(temp.getRequest());
						temp.getReplyTo().tell(granted, getSelf());
					}
				}
				else {
					// resource isnt local
					log(LogMsg.makeManagementRequestForwardedLogMsg(getSelf(), man, temp.getRequest()));
					man.tell(msg, getSender());
				}
			}

		}
		else if(msg instanceof WhoHasResourceRequestMsg) {
			WhoHasResourceRequestMsg temp = (WhoHasResourceRequestMsg)msg;
			Boolean result = false;
			if(localResources.containsKey(temp.getResourceName())) {
				result = true;
			}
			WhoHasResourceResponseMsg response = new WhoHasResourceResponseMsg(temp.getResourceName(),result,getSelf());
			getSender().tell(response, getSelf());
		}
		else if(msg instanceof WhoHasResourceResponseMsg) {
			WhoHasResourceResponseMsg temp = (WhoHasResourceResponseMsg)msg;
			ArrayList<Object> request = null;
			String name = temp.getResourceName();
			// check that the src hasnt already been found and removed from the list previously
			if(srcSearch.containsKey(name)) {
				if(temp.getResult()) {
					// send appropriate request to the sender of this response
					// also add the found resource to that manager in your hashmap srcToManager
					request = waitForSearch.remove(name);
					srcSearch.remove(name);
					srcToManager.put(name, getSender());
					// send log for discovered resource
					log(LogMsg.makeRemoteResourceDiscoveredLogMsg(getSelf(),getSender(),name));
					for(Object o: request) {
						handleWhoHas(o,temp.getSender());
					}
				}
				else {
					// increment the value for request rejections
					srcSearch.computeIfPresent(name,(key,val) -> val + 1 );
					// if every manager says they dont have it remove the request and send the appropriate response
					if(srcSearch.get(name) == managers.size() - 1) {
						srcSearch.remove(name);
						request = waitForSearch.remove(name);
						for(Object o : request) {
							handleWhoHas(o,null);
						}
					}
				}
			}
		}
	}
}
