package com.redislabs.quartz;

public enum RedisTriggerState {
	WAITING("waiting_triggers"),
	PAUSED("paused_triggers"),
	BLOCKED("blocked_triggers"),
	PAUSED_BLOCKED("paused_blocked_triggers"),
	ACQUIRED("acquired_triggers"),
	COMPLETED("completed_triggers"),
	ERROR("error_triggers");
	
	private final String key;
	
	private RedisTriggerState(String key) {
		this.key = key;
	}
	
	public String getKey() { return key; }
	
	public static RedisTriggerState toState(String key){
		for (RedisTriggerState state : RedisTriggerState.values()) 
			if (state.getKey().equals(key))
				return state;
		
		return null;
	}	
}