package se.kth.swim;

import java.util.UUID;

import se.sics.p2ptoolbox.util.network.NatedAddress;

public class VicinityEntry {
	
	public VicinityEntry(NatedAddress node) {
		super();
		this.nodeAdress = node;
	}
	
	public NatedAddress nodeAdress = null;
	public boolean waitingForPong = false;
	public int waitingForPongCount = 0;
	//public UUID pongCheckTimeoutId;
	public String nodeStatus = "LIVE"; // LIVE - SUSPECTED - DEAD 
	
	
	
}
