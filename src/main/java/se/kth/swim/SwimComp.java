/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * GVoD is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.kth.swim;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import javassist.bytecode.Descriptor.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kth.swim.msg.Ping;
import se.kth.swim.msg.Pong;
import se.kth.swim.msg.PingReq;
import se.kth.swim.msg.PongReq;
import se.kth.swim.msg.Ping2ndHand;
import se.kth.swim.msg.Pong2ndHand;
import se.kth.swim.msg.Status;
import se.kth.swim.msg.net.NetPing;
import se.kth.swim.msg.net.NetPong;
import se.kth.swim.msg.net.NetPing2ndHand;
import se.kth.swim.msg.net.NetPong2ndHand;
import se.kth.swim.msg.net.NetPingReq;
import se.kth.swim.msg.net.NetPongReq;
import se.kth.swim.msg.net.NetStatus;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.p2ptoolbox.util.network.NatedAddress;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class SwimComp extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(SwimComp.class);
    private Positive<Network> network = requires(Network.class);
    private Positive<Timer> timer = requires(Timer.class);

    private final NatedAddress selfAddress;
    private final Set<NatedAddress> bootstrapNodes;
    private final NatedAddress aggregatorAddress;

    private UUID pingTimeoutId;
    private UUID statusTimeoutId;
    private UUID pongCheckTimeoutId;
    
    
    private int sentPings = 0;
    private int receivedPongs = 0;    
    private int receivedPings = 0;
    
    
    //-- Riz
    private int pingBufferPointer = 0;
    private List<VicinityEntry> vicinityNodeList = new ArrayList<VicinityEntry>();
    private List<NatedAddress> joinedNodeList = new ArrayList<NatedAddress>();
    private Set<NatedAddress> deletedNodeList = new HashSet<NatedAddress>();
    
    //--

    public SwimComp(SwimInit init) {
        this.selfAddress = init.selfAddress;
        log.info("{} initiating...", selfAddress);
        this.bootstrapNodes = init.bootstrapNodes;
        this.aggregatorAddress = init.aggregatorAddress;

        subscribe(handleStart, control);
        subscribe(handleStop, control);
        subscribe(handlePing, network);
        subscribe(handlePingReq, network);
        subscribe(handlePongReq, network);
        subscribe(handlePing2ndHand, network);
        subscribe(handlePong2ndHand, network);        
        subscribe(handlePong, network);
        subscribe(handlePingTimeout, timer);
        subscribe(handleStatusTimeout, timer);
        subscribe(handlePongCheckTimeout, timer);
        
    }

    private Handler<Start> handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            log.info("{} starting...", new Object[]{selfAddress.getId()});          
            
            //-- Riz [ List the partners for this node ].
            
            if (!bootstrapNodes.isEmpty()) {
            	
            	// Transfer the bootstrapNodes to the vicinityNodeList            	
            	for (NatedAddress partnerAddress : bootstrapNodes) {
            		//log.info("Partners from boot: {} ", partnerAddress.getId());
            		vicinityNodeList.add( new VicinityEntry(partnerAddress));
            	}      
            	
            	for (java.util.Iterator<VicinityEntry> iterator = vicinityNodeList.iterator(); iterator.hasNext();)
            	{
            		log.info("Partners from boot: {} ", ((VicinityEntry)iterator.next()).nodeAdress.getId());            		            		
            	}                
            }
        	
            schedulePeriodicPing();
            schedulePeriodicPongCheck();
            schedulePeriodicStatus();
            
            // --
        }

    };
    private Handler<Stop> handleStop = new Handler<Stop>() {

        @Override
        public void handle(Stop event) {
            log.info("{} stopping...", new Object[]{selfAddress.getId()});
            if (pingTimeoutId != null) {
                cancelPeriodicPing();
            }
            if (statusTimeoutId != null) {
                cancelPeriodicStatus();
            }
            // -- Riz
            if (pongCheckTimeoutId != null) {
                cancelPeriodicPongCheck();
            }
            // --
            
        }

    };

    // Handling Ping
    private Handler<NetPing> handlePing = new Handler<NetPing>() {

        @Override
        public void handle(NetPing event) {
        	// -- Riz
            //log.info("{} received ping from:{} ", new Object[]{selfAddress.getId(), event.getHeader().getSource()});
            receivedPings++;            
            
            // PIGGY-BACK AND PONG:  Piggyback pong with Newly joined nodes 
            //log.info("{} replying pong to:{}", new Object[]{selfAddress.getId(), event.getHeader().getSource()});            
            Set<NatedAddress> _piggyBackedJoinedNodes = new HashSet<NatedAddress>();
            for (NatedAddress newNode : joinedNodeList) 
            	_piggyBackedJoinedNodes.add(newNode);            
            trigger(new NetPong(selfAddress, event.getHeader().getSource(),new Pong(_piggyBackedJoinedNodes,deletedNodeList )), network);
            
            // ADD PINGER: If the pinger is not in the vicinity-list - add him in the vicinityNodeList and joinedNodeList
            // Reason behind adding the pinger in the joinedNodeList after triggering the pong is:
            //   The pinger itself doesn't need to be piggy-backed back to itself.  
            if (!vicinityNodeList.contains(event.getHeader().getSource())){
            	AddUniqueToVicinity(event.getHeader().getSource());
                AddUniqueToJoinedList(event.getHeader().getSource());
            }                        
            // --
        }

    };
    
    // Handling Pong
    private Handler<NetPong> handlePong = new Handler<NetPong>() {

        @Override
        public void handle(NetPong event) {        	
        	
            log.info("{} received pong from:{} with {} " + ProcessSet(event.getContent().getPiggyBackedJoinedNodes()) + " new and {} " + ProcessSet(event.getContent().getPiggyBackedDeadNodes()) +  "dead nodes.", new Object[]{selfAddress.getId(), event.getHeader().getSource(),event.getContent().getPiggyBackedJoinedNodes().size(),event.getContent().getPiggyBackedDeadNodes().size()});
            receivedPongs++;
            
            //CHANGE STATUS: Change the status of the node in vicinity-list as NOT waitingForPong  
            for (VicinityEntry _vNode: vicinityNodeList){
            	if (_vNode.nodeAdress == event.getSource()){            	
            		_vNode.waitingForPong = false;            		
            	    _vNode.waitingForPongCount = 0;
            	    if (_vNode.nodeStatus == "SUSPECTED"){
            	    	_vNode.nodeStatus = "LIVE";
            	    }
            	}            	
            }
            
            // MARGE PIGGY-BACK: If the piggy-backed new nodes are not in the vicinityNodeList - add them 
            if (event.getContent().getPiggyBackedJoinedNodes().size() > 0){
            	for(NatedAddress newPiggyBackedNode : event.getContent().getPiggyBackedJoinedNodes()){
            		if (newPiggyBackedNode != selfAddress ){
            			AddUniqueToVicinity(newPiggyBackedNode);
                		AddUniqueToJoinedList(newPiggyBackedNode);	
            		}            		            		
            	}
            }
            // MARGE PIGGY-BACK: If the piggy-backed dead node are in the vicinityNodeList -  remove them
            if (event.getContent().getPiggyBackedDeadNodes().size() > 0){
            	for(NatedAddress deadPiggyBackedNode : event.getContent().getPiggyBackedDeadNodes()){
            		if (deadPiggyBackedNode != selfAddress ){
            			RemoveFromVicinityList(deadPiggyBackedNode);
            			deletedNodeList.add(deadPiggyBackedNode );                			
            		}            		            		
            	}
            }
            
        }
    };

    
    
    // Timer Ping 
    private Handler<PingTimeout> handlePingTimeout = new Handler<PingTimeout>() {

        @Override
        public void handle(PingTimeout event) {
        	//-- Riz 
        	/*
        	 * Broadcast ping
        	 */
//            for (NatedAddress partnerAddress : bootstrapNodes) {            	
//                log.info("{} sending ping to partner:{}", new Object[]{selfAddress.getId(), partnerAddress});
//                trigger(new NetPing(selfAddress, partnerAddress), network);
//            }
        	/*
        	 *	 Selective Ping
        	 */

        	// FILTER: Filter out the waiting-for-pong nodes
        	// SUSPEND: Suspend nodes if too long waiting counter
        	Set<NatedAddress> _pingCandidates = new HashSet<NatedAddress>(); 
        	VicinityEntry _justSuspected = null;
        	VicinityEntry _justDead = null;
            for (VicinityEntry vNode : vicinityNodeList){
            	if (vNode.waitingForPong == true){
            		//if (vNode.nodeStatus == "LIVE"){
            			log.info("{} missed pong from {} by {} times", new Object[]{selfAddress.getId(),vNode.nodeAdress , vNode.waitingForPongCount });
            			vNode.waitingForPongCount ++;
            			
            		//}            			
            		// Suspecting in less than 2 cycle-time or (2*1000) millisecond
            		if (vNode.nodeStatus == "LIVE" && vNode.waitingForPongCount >= 2 ){            			
            			log.info("{} detected no response from {} ", new Object[]{selfAddress.getId(), vNode.nodeAdress });
            			vNode.nodeStatus = "SUSPECTED";
            			_justSuspected = vNode;
            			
            		}  
            		// Dead declare in less than 3 cycle-time or (3*1000) millisecond
            		if (vNode.nodeStatus == "SUSPECTED" && vNode.waitingForPongCount >= 3 ){
            			log.info("{} detected dead node {} ", new Object[]{selfAddress.getId(), vNode.nodeAdress });
            			vNode.nodeStatus = "DEAD";  
            			_justDead = vNode;
            		}            		
            	}
            	else{
            		_pingCandidates.add(vNode.nodeAdress);
            	}            		
            }
            //REMOVE-DEAD: Remove the DEAD
            if (_justDead != null){
    			// Remove from vicinity.
    			RemoveFromVicinityList(_justDead.nodeAdress);
    			// Add in dead-list to be piggy-backed.
    			deletedNodeList.add(_justDead.nodeAdress);
            }
            
            
            
    		// PING-REQ: Trigger a Ping-Req for this suspected node to random K nodes in vicinity list
            if (_justSuspected != null){
            	for (VicinityEntry vNode : vicinityNodeList){
            		if (vNode != _justSuspected && vNode.nodeStatus == "LIVE"){
            			// select random k nodes ...
            			// ..
            			trigger(new NetPingReq(selfAddress, vNode.nodeAdress, new PingReq(_justSuspected.nodeAdress)), network);            			
            		}            			
            	}
            }
        	
        	
        	// PING: ping the ping candidates 
        	if (!_pingCandidates.isEmpty() )
        	{
        		log.info("{} has ping cnadidates {} ", new Object[]{selfAddress.getId(),_pingCandidates.size()  });
        		// If the set with single element, ping that
        		if (_pingCandidates.size() == 1 ){
        			// Ping the node
        			goPingTheNode(_pingCandidates.iterator().next());
        		}
        		// Pick a random element to ping
        		else{
        			int size = _pingCandidates.size();
        			int item = new Random().nextInt(size); // In real life, the Random object should be rather more shared than this
        			log.info("{} has ping cnadidates {}, going to ping {}th ", new Object[]{selfAddress.getId(),size,item  });
        			int i = 0;        			
        			for(NatedAddress pingPartner : _pingCandidates)
        			{
        			    if (i == item){        			    	
        			    	// Ping the node
        			    	goPingTheNode(pingPartner);
        			    	break;
        			    }   
        			    i = i + 1;
        			}
        		}        		
        	}
            //--
        }

    };

    // Timer Status
    private Handler<StatusTimeout> handleStatusTimeout = new Handler<StatusTimeout>() {

        @Override
        public void handle(StatusTimeout event) {
            //log.info("{} sending status to aggregator:{}", new Object[]{selfAddress.getId(), aggregatorAddress});
            trigger(new NetStatus(selfAddress, aggregatorAddress, new Status(sentPings,receivedPings, receivedPongs, vicinityNodeList)), network);
        }
    };
    
        
    // -- Riz
    
    // Handling PingReq
    private Handler<NetPingReq> handlePingReq = new Handler<NetPingReq>() {

        @Override
        public void handle(NetPingReq event) {      	
        	
        	log.info("{} received ping-req from:{} for {} ", new Object[]{selfAddress.getId(), event.getHeader().getSource(),event.getContent().GetTestSubjectNode()});        	
        	trigger(new NetPing2ndHand(selfAddress, event.getContent().GetTestSubjectNode(), new Ping2ndHand(event.getHeader().getSource())), network);
        }

    };
    
    // Handle Pong-Req
    private Handler<NetPongReq> handlePongReq = new Handler<NetPongReq>() {

        @Override
        public void handle(NetPongReq event) {      	
        	
        	log.info("{} received pong-req from:{} for {} ", new Object[]{selfAddress.getId(), event.getHeader().getSource(),event.getContent().GetTestSubjectNode()});        	
        	NatedAddress _suspectedNode = event.getContent().GetTestSubjectNode();
        	// check in the vicinity list if _suspectedNode is SUSPECTED then make it LIVE. 
        	for (VicinityEntry partner : vicinityNodeList){
        		if (partner.nodeAdress == _suspectedNode && partner.nodeStatus == "SUSPECTED"){
        			partner.waitingForPong = false;
        			partner.waitingForPongCount = 0;
        			partner.nodeStatus = "LIVE";
        		}
        	}
        }

    };
    
    
    // Handling Ping2ndHand
    private Handler<NetPing2ndHand> handlePing2ndHand = new Handler<NetPing2ndHand>() {

        @Override
        public void handle(NetPing2ndHand event) {
        	
            log.info("{} received 2nd-hand-ping from:{} with caller {} ", new Object[]{selfAddress.getId(), event.getHeader().getSource(),event.getContent().GetTestRequesterNode()});
            trigger(new NetPong2ndHand(selfAddress, event.getHeader().getSource(), new Pong2ndHand(event.getContent().GetTestRequesterNode())) ,network);            
        }
    };

    
    // Handling Pong2ndHand
    private Handler<NetPong2ndHand> handlePong2ndHand = new Handler<NetPong2ndHand>() {

        @Override
        public void handle(NetPong2ndHand event) {
        	
            log.info("{} received 2nd-hand-pong from:{} with caller {} ", new Object[]{selfAddress.getId(), event.getHeader().getSource(),event.getContent().GetTestRequesterNode()});
            trigger(new NetPongReq(selfAddress, event.getContent().GetTestRequesterNode(), new PongReq(event.getHeader().getSource())) ,network);            
        }
    };

    
    
    
    // Timer Pong check
    private Handler<PongCheckTimeout> handlePongCheckTimeout = new Handler<PongCheckTimeout>() {

        @Override
        public void handle(PongCheckTimeout event) {
            //log.info("{} checking pending ping ... ", new Object[]{selfAddress.getId()});
        	/*
        	VicinityEntry _justSuspected = null;
            for (VicinityEntry vNode : vicinityNodeList){
            	if (vNode.waitingForPong == true){
            		if (vNode.nodeStatus == "LIVE"){
            			vNode.waitingForPongCount ++;
            		}            			
            		// Suspecting in less than (2*1000) millisecond
            		if (vNode.waitingForPongCount >= 2 && vNode.nodeStatus == "LIVE" ){            			
            			log.info("{} detected no response from {} ", new Object[]{selfAddress.getId(), vNode.nodeAdress });
            			vNode.nodeStatus = "SUSPECTED";
            			_justSuspected = vNode; 
            			
            		}             			
            	}
            }   
    		// Trigger a Ping-Req for this suspected node to random K nodes in vicinity list
            if (_justSuspected != null){
            	for (VicinityEntry vNode : vicinityNodeList){
            		if (vNode != _justSuspected && vNode.nodeStatus == "LIVE"){
            			// select random k nodes ...
            			// ..
            			trigger(new NetPingReq(selfAddress, vNode.nodeAdress, new PingReq(_justSuspected.nodeAdress)), network);            			
            		}            			
            	}
            }
            */
            
        }
    };

    private void schedulePeriodicPongCheck() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(1000, 1000);
        PongCheckTimeout sc = new PongCheckTimeout(spt);
        spt.setTimeoutEvent(sc);
        pongCheckTimeoutId = sc.getTimeoutId();
        trigger(spt, timer);
    }  

	private void cancelPeriodicPongCheck() {
        CancelTimeout cpt = new CancelTimeout(pongCheckTimeoutId);
        trigger(cpt, timer);
        pongCheckTimeoutId = null;
    }
    
//    private void schedulePeriodicSuspectedCheck(NatedAddress suspectedNode) {
//        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(2000, 2000);
//        SuspectedCheckTimeout sc = new SuspectedCheckTimeout(spt, suspectedNode);
//        spt.setTimeoutEvent(sc);
//        suspectedTimeoutId = sc.getTimeoutId();
//        trigger(spt, timer);
//    }
    
    // --
    

    private void schedulePeriodicPing() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(1000, 1000);
        PingTimeout sc = new PingTimeout(spt);
        spt.setTimeoutEvent(sc);
        pingTimeoutId = sc.getTimeoutId();
        trigger(spt, timer);
    }    


	private void cancelPeriodicPing() {
        CancelTimeout cpt = new CancelTimeout(pingTimeoutId);
        trigger(cpt, timer);
        pingTimeoutId = null;
    }
	
	

    private void schedulePeriodicStatus() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(10000, 10000);
        StatusTimeout sc = new StatusTimeout(spt);
        spt.setTimeoutEvent(sc);
        statusTimeoutId = sc.getTimeoutId();
        trigger(spt, timer);
    }

    private void cancelPeriodicStatus() {
        CancelTimeout cpt = new CancelTimeout(statusTimeoutId);
        trigger(cpt, timer);
        statusTimeoutId = null;
    }

    public static class SwimInit extends Init<SwimComp> {

        public final NatedAddress selfAddress;
        public final Set<NatedAddress> bootstrapNodes;
        public final NatedAddress aggregatorAddress;

        public SwimInit(NatedAddress selfAddress, Set<NatedAddress> bootstrapNodes, NatedAddress aggregatorAddress) {
            this.selfAddress = selfAddress;
            this.bootstrapNodes = bootstrapNodes;
            this.aggregatorAddress = aggregatorAddress;
        }
    }

    private static class StatusTimeout extends Timeout {

        public StatusTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    private static class PingTimeout extends Timeout {

        public PingTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    // -- Riz
    
    private static class PongCheckTimeout extends Timeout {

        public PongCheckTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }
    
    private void goPingTheNode(NatedAddress pingPartner) {
		log.info("{} sending ping to partner:{} with vicinity - " + ProcessVicinityList(vicinityNodeList), new Object[]{selfAddress.getId(), pingPartner.getId()});
    	//log.info("{} sending ping to partner:{} ", new Object[]{selfAddress.getId(), pingPartner.getId()});
    	trigger(new NetPing(selfAddress, pingPartner), network);
    	sentPings++;
    	
    	// Mark the node as waiting for ping in vicinity list
      for (VicinityEntry partner : vicinityNodeList){
      	if (partner.nodeAdress == pingPartner)
      		partner.waitingForPong = true;
      }
	
    }
    
    public String ProcessSet(Set<NatedAddress> vList)
    {
    	String st = " ";    	
    	for(NatedAddress nd : vList){
    		
    		st += nd.getId() + " ";
    	}
    	st = "[" + st + "]"; 
    	return st;
    	    	
    }
    
    public String ProcessVicinityList(List<VicinityEntry> vList)
    {
    	String st = " ";
    	
    	for(VicinityEntry nd : vList){
    		String stat = "";
    		if (nd.nodeStatus == "SUSPECTED"){
    			stat = "(S" + nd.waitingForPongCount + ")";
    		}
    		st += nd.nodeAdress.getId() + stat + " ";
    	}
    	return st;
    	    	
    }
    


	protected void AddUniqueToJoinedList(NatedAddress node) {
		if (joinedNodeList.size() > 0){
    		if (!joinedNodeList.contains(node)){
    			joinedNodeList.add(node);
    		}   			
    		
    	}else{
    		joinedNodeList.add(node);
    	}
	}
    
    protected void AddUniqueToVicinity(NatedAddress node) {

    	if (vicinityNodeList.size() > 0){
    		boolean _contains = false;
    		for(VicinityEntry entry : vicinityNodeList){
    			if (entry.nodeAdress == node)
    				_contains = true;
    		}
    		if (!_contains )
    			vicinityNodeList.add(new VicinityEntry(node));
    		
   			    		
    	}else{
    		vicinityNodeList.add(new VicinityEntry(node));
    	}
		
	}
    
    protected void RemoveFromVicinityList(NatedAddress deletedNode) {
    	if (vicinityNodeList.size() > 0){
    		VicinityEntry _temp = null;    		
    		for(VicinityEntry entry : vicinityNodeList){
    			if (entry.nodeAdress == deletedNode){
    				_temp = entry;    				
    			}    			
    		}
    		if (_temp != null){
    			vicinityNodeList.remove(_temp);
    		}
    		
    	}		
	}
    
//    protected void MargeNeighbors(Set<NatedAddress> p_PiggyBackNodes) {
//    	if (p_PiggyBackNodes.size() > 0)
//    	{
//    		for (NatedAddress piggyBackedAddress : p_PiggyBackNodes) {
//    			int _contains = 0;
//    			for (NatedAddress localAddress : bootstrapNodes) {        		
//    				if (localAddress == piggyBackedAddress )
//    					_contains = 1;
//        		}
//    			if (_contains == 0)
//    				bootstrapNodes.add(piggyBackedAddress);    			
//    			
//        	}    		
//    	}
//    	if (!bootstrapNodes.isEmpty()) {
////        	//-- Riz [ List the partners for this node ].           	
////        	for (NatedAddress partnerAddress : bootstrapNodes) {
////        		log.info("Partners for me {} ", partnerAddress.getId());
////        	}            	
////        	//--
//            schedulePeriodicPing();
//        }
//    	
//	}
    // --
    
}
