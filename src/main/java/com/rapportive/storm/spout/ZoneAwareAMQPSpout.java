package com.rapportive.storm.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import backtype.storm.spout.Scheme;

import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rapportive.storm.amqp.QueueDeclaration;
import com.rapportive.storm.spout.AMQPSpout;

public class ZoneAwareAMQPSpout extends AMQPSpout {

    private static final long serialVersionUID = 11258640002629263L;

    private static final Logger log = Logger.getLogger(ZoneAwareAMQPSpout.class);
    
	public ZoneAwareAMQPSpout( String host, int port, String username, String password, String vhost, 
			QueueDeclaration queueDeclaration, Scheme scheme, boolean requeueOnFail ) {
		
		super( host, port, username, password, vhost, queueDeclaration, scheme, requeueOnFail );
	}
	
	@Override
	protected void setupAMQP() throws IOException {
		
		System.out.println( "In setupAMQP for zone-aware spout" );
		final int prefetchCount = this.prefetchCount;

		final ConnectionFactory connectionFactory = new ConnectionFactory();

		// set fallbacks
		connectionFactory.setHost( amqpHost );
		connectionFactory.setPort( amqpPort );
	
		System.out.println( "In setupAMQP for zone-aware spout before reading etc/app.env" );
		BufferedReader br = new BufferedReader( new InputStreamReader( new FileInputStream( new File( "/etc/app.env" ) ) ) );
		
		try {
		    String line;
		    while ((line = br.readLine()) != null) {
		        if( line.contains( "AMQP_HOST" ) ) {
		        	//process line
		        	String hostStr = line.split( "=" )[1].replaceAll( "'", "" ).trim();
		        			
		        	connectionFactory.setHost( hostStr );
		        	
		        	System.out.println( "Setting host to " + hostStr + " as a zone specific setting from app.env" );
		        	continue;
		        }
		        
		        if( line.contains( "AMQP_PORT" ) ) {
		        	//process line
		        	Integer port = Integer.parseInt( line.split( "=" )[1].replaceAll( "'", "" ).trim() );
		        			
		        	connectionFactory.setPort( port );
		        	
		        	System.out.println( "Setting port to " + port + " as a zone specific setting from app.env" );
		        	continue;
		        }
		    }
		} finally {
		    br.close();
		}
		

		System.out.println( "In setupAMQP for zone-aware spout after setting zone specific settings" );

		connectionFactory.setUsername( amqpUsername );
		connectionFactory.setPassword( amqpPassword );
		connectionFactory.setVirtualHost( amqpVhost );

		this.amqpConnection = connectionFactory.newConnection();
		this.amqpChannel = amqpConnection.createChannel();

        log.info("Setting basic.qos prefetch-count to " + prefetchCount);
        amqpChannel.basicQos(prefetchCount);

        final Queue.DeclareOk queue = queueDeclaration.declare(amqpChannel);
        final String queueName = queue.getQueue();
        log.info("Consuming queue " + queueName);

        this.amqpConsumer = new QueueingConsumer(amqpChannel);
        this.amqpConsumerTag = amqpChannel.basicConsume(queueName, false /* no auto-ack */, amqpConsumer);
    }
}
