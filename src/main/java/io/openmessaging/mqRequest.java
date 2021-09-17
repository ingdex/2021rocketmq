package io.openmessaging;
import java.lang.String;
import java.nio.ByteBuffer;
import java.io.*;
import java.util.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.HashMap;
import org.json.JSONObject;

public class mqRequest {
    String topic;
    Integer queueId;
    Long offset;
    ByteBuffer data;
    mqRequest() {
	};
 
	mqRequest(String topic, Integer queueId, Long offset, ByteBuffer data) {
		this.topic = topic;
		this.queueId = queueId;
		this.offset = offset;
		this.data = data;
	}

	@Override
	public String toString() {
		return topic + " " +
				String.valueOf(queueId) + " " +
				String.valueOf(offset) + " " +
				String.valueOf(data);
	}
}
