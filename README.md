# MagistralJava
MagistralJava is a messaging library written in Java.

Features
Requirements
Dependencies
Usage

- [Features](#features)
- [Requirements](#requirements)
- [Dependencies](#dependencies)
- [Installation](#installation)
- [Usage](#usage)
  - **Intro -** [Prerequisites](#prerequisites), [Key-based access](#key-based-access) 
  - **Connecting -** [Connecting](#connecting)
  - **Resources -** [Topics](#topics)
  - **Publish / Subscribe -** [Publish](#publish), [Subscribe](#subscribe)
  - **History -** [History](#history), [History for Time Interval](#history-for-time-interval)
  - **Access Control -** [Permissions](#permissions), [Grant permissions](#grant-permissions), [Revoke permissions](#revoke-permissions)
  
## Features

- [x] Send / receive data messages
- [x] Replay (Historical data) 
- [x] Resource discovery
- [x] Access Control
- [x] TLS-encrypted communication
- [x] Client-side AES-encryption

## Requirements

- Java 1.8+

## Dependencies

- [jersey-client](https://jersey.java.net/) [1.19.3]
- [jersey-apache-client4](https://mvnrepository.com/artifact/com.sun.jersey.contribs/jersey-apache-client4/1.19.3) [1.19.3]
- [kafka-clients](http://kafka.apache.org/) [0.10.2.0]
- [json-simple](https://mvnrepository.com/artifact/com.googlecode.json-simple/json-simple/1.1.1) [1.1.1]
- [Paho](http://www.eclipse.org/paho/) [1.1.0]

## Installation

### Manually

MagistralJava is currently available only in GitHub. Please check out the SDK and connect it to your project as source. 

---

## Usage

### Prerequisites

First of all, to stream data over Magistral Network you need to have an Application created.
If you don't have any of them yet, you can easily create one from [Customer Management Panel](https://app.magistral.io) 
(via start page or in [Application Management](https://app.magistral.io/#/apps) section).

Also, you need to have at least one topic created, that you can do from [Topic Management](https://app.magistral.io/#/topics) panel.

### Key-based access

Access to the Magistral Network is key-based. There are three keys required to establish connection:
  - **Publish Key** - Application-specific key to publish messages.
  - **Subscribe Key** - Application-specific key to read messages.
  - **Secret Key** - User-specific key to identify user and his permissions.

You can find both **Publish Key** and **Subscribe Key** in [Application Management](https://app.magistral.io/#/apps) section.
Select your App in the list and click Clipboard icons in App panel header to copy these keys into the Clipboard. 

**Secret Key** - can be found among user permissions in [User Management](https://app.magistral.io/#/usermanagement/) section.
You can copy secret key linked to user permissions into the Clipboard, just clicking right button in the permission list.

### Connecting
To establish connection with Magistral Network you need to create Magistral instance and provide **pub**, **sub** and **secret** keys.

```Java
import io.magistral.client.Magistral;

final Magistral m = new Magistral(
    pubKey: "pub-xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    subKey: "sub-xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    secretKey: "s-xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx");
```

### Topics

To discover all available topics and channels you can call:
```java

m.topics(new io.magistral.client.topics.Callback() {			
	public void success(List<TopicMeta> meta) {				
		for (TopicMeta m : meta) {
			String topic = m.getTopicName();
			Set<Integer> channels = m.getСhannels();
//			Do something with topic and channels
		}
	}
				
	public void error(MagistralException ex) {}
});

```

In case you know the topic name and want to see information about channels:
```java
m.topic("{topic}", new io.magistral.client.topics.Callback() {			
	public void success(List<TopicMeta> meta) {				
		for (TopicMeta m : meta) {
			String topic = m.getTopicName();
			Set<Integer> channels = m.getСhannels();
//			Do something with topic and channels
		}
	}
				
	public void error(MagistralException ex) {}
});
```

### Publish

You can send data message to Magistral in this way:

```java
String topic = "topic"
int channel = 0

m.publish(topic, channel, (message).getBytes(StandardCharsets.UTF_8), new Callback() {
								
	public void success(PubMeta meta) {
//		System.out.println("✔︎ Published to " + ack.topic() + ":" + ack.channel());
	}
								
	public void error(MagistralException ex) {}
});

```

### Subscribe

This is an example how to subscribe and handle incoming data messages:
```java
String topic = "topic"
String group = "leader"

m.subscribe(topic, group, new NetworkListener() {
	
	public void messageReceived(MessageEvent e) {
		String topic = e.topic();
		int channel = e.channel();
		byte[] message = e.msgBody();
							
		String msString = new String(message, StandardCharsets.UTF_8);
		System.out.println(c.get() + " -> " + e.index() + " :: received [" + msString + "] on '" + topic + "' channel '" + channel);
							
	}

	public void disconnected(String topic) {}
	public void reconnect(String topic) {}
	public void error(MagistralException ex) {}
	public void connected(String topic) {}
	
}, new io.magistral.client.sub.Callback() {			
	
	public void success(SubMeta meta) {}						
	public void error(Throwable ex) {}
});
```
### History

Magistral allows you to replay data  sent via some specific topic and channel. This feature called **History**.
To see last n-messages in the channel:
```java
String topic = "topic"
int channel = 0
int count = 100

m.history(topic, channel, count, new io.magistral.client.data.Callback() {			
	public void success(History history) {		
		for (Message m : history.getMessages()) {
			System.out.println(m.getChannel() + " : " + "[" + m.getTimestamp() + "] / " + new String(m.getBody(), StandardCharsets.UTF_8));
		}
	}
					
	public void error(MagistralException ex) {}
});
```

You can also provide timestamp to start looking messages from:
```java
String topic = "topic"
int channel = 0
int count = 100
long start = new Date(System.currentTimeMillis() - 6 * 60 * 60 * 1000);

m.history(topic, channel, start, count, new io.magistral.client.data.Callback() {			
	public void success(History history) {		
		for (Message m : history.getMessages()) {
			System.out.println(m.getChannel() + " : " + "[" + m.getTimestamp() + "] / " + new String(m.getBody(), StandardCharsets.UTF_8));
		}
	}
					
	public void error(MagistralException ex) {}
});
```
### History for Time Interval

Historical data in Magistral can be obtained also for some period of time. You need to specify start and end date:
```java
String topic = "topic"
int channel = 0
int count = 100
long start = new Date(System.currentTimeMillis() - 6 * 60 * 60 * 1000);
long end = new Date(System.currentTimeMillis() - 4 * 60 * 60 * 1000);

m.history(topic, channel, start, end, new io.magistral.client.data.Callback() {			
	public void success(History history) {		
		for (Message m : history.getMessages()) {
			System.out.println(m.getChannel() + " : " + "[" + m.getTimestamp() + "] / " + new String(m.getBody(), StandardCharsets.UTF_8));
		}
	}
					
	public void error(MagistralException ex) {}
});

### Permissions

This is a part of Access Control functionality. First of all, to see the full list of permissions:

```java
m.permissions(new io.magistral.client.perm.Callback() {	
			
	public void success(List<PermMeta> meta) {
		if (meta.size() == 0) {
			System.out.println("No permissions");
			return;
		}
		PermMeta perm = meta.get(0);
		for (int ch : perm.channels()) {
			System.out.println("[" + ch + "] ~> " + perm.readable(ch) + " : " + perm.writable(ch));
		}
	}

	public void error(MagistralException ex) {}
});
```

Or if you are interested to get permissions for some specific topic:

```swift
m.permissions("topic", new io.magistral.client.perm.Callback() {	
			
	public void success(List<PermMeta> meta) {
		if (meta.size() == 0) {
			System.out.println("No permissions");
			return;
		}
		PermMeta perm = meta.get(0);
		for (int ch : perm.channels()) {
			System.out.println("[" + ch + "] ~> " + perm.readable(ch) + " : " + perm.writable(ch));
		}
	}

	public void error(MagistralException ex) {}
});
```

### Grant permissions

You can grant permissions for other users directly from SDK:
```java
String user = "user";
String topic = "topic";
int channel = 5;
boolean read = true;
boolean write = true;

mag.grant(user, topic, channel, read, write, new io.magistral.client.perm.Callback() {			
				
	public void success(List<PermMeta> meta) {
		System.out.println("Updated user permissions : ");
		for (PermMeta p : meta) {
			System.out.println(p.topic() + " " + p.channels());
		}
	}
				
	public void error(MagistralException ex) {}
});
```
> You must have super user priveleges (credentials you provided when signed up) to execute this function.

### Revoke permissions

In similar way you can revoke user permissions:
```java
String user = "user";
String topic = "topic";
int channel = 5;

mag.revoke(user, topic, channel, new io.magistral.client.perm.Callback() {			
				
	public void success(List<PermMeta> meta) {
		System.out.println("Updated user permissions : ");
		for (PermMeta p : meta) {
			System.out.println(p.topic() + " " + p.channels());
		}
	}
				
	public void error(MagistralException ex) {}
});
```
> You must have super user priveleges (credentials you provided when signed up) to execute this function.

## License
Magistral is released under the MIT license. See LICENSE for details.

