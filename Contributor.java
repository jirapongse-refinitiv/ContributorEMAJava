///*|----------------------------------------------------------------------------------------------------
// *|            This source code is provided under the Apache 2.0 license      	--
// *|  and is provided AS IS with no warranty or guarantee of fit for purpose.  --
// *|                See the project's LICENSE.md for details.                  					--
// *|           Copyright Refinitiv 2020. All rights reserved.            		--
///*|----------------------------------------------------------------------------------------------------

import com.refinitiv.ema.access.*;
import com.refinitiv.ema.domain.login.Login.LoginReq;
import com.refinitiv.ema.rdm.EmaRdm;

class AppClient implements OmmConsumerClient	{
	private OmmConsumer _ommConsumer;
	private long _tunnelStreamHandle = 0, _subStreamHandle;
	private int _postStreamID;
	private boolean _subItemOpen;
	int _postID = 1;
	int _bid = 340, _ask = 341;
	//private boolean _connectionUp = false;
	
//	public void setLoginHandle(long loginhandle) {
//		_loginStreamHandle = loginhandle;
//	}
	public void  onSourceDirectoyUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent event) {
		System.out.println("-------- On Source Directory Update ----------------");
		System.out.println(_tunnelStreamHandle);
		if(_tunnelStreamHandle == 0) {
			System.out.println("Start Tunnel");
			StartTunnelStream();
		}
		System.out.println("----------------------------------------------------");
	}
	
	public void  onSourceDirectoyRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent event) {
		System.out.println("-------- On Source Directory Refresh ----------------");
		System.out.println(_tunnelStreamHandle);
		if(_tunnelStreamHandle == 0) {
			System.out.println("Start Tunnel");
			StartTunnelStream();
		}
		System.out.println("----------------------------------------------------");
	}
	public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent event)	{
		System.out.println("----- Refresh message ----");
			System.out.println(refreshMsg);
		
//		if(refreshMsg.domainType()==EmaRdm.MMT_LOGIN && event.handle()==_loginStreamHandle) {
//				onLoginRefreshMsg(refreshMsg, event);
//		}
		
		if(refreshMsg.domainType() == EmaRdm.MMT_DIRECTORY) {
			onSourceDirectoyRefreshMsg( refreshMsg,  event);
		}
			
		System.out.println(event.handle());
		System.out.println(_tunnelStreamHandle);
		System.out.println(_subItemOpen);
		if(_subItemOpen && event.handle() == _subStreamHandle && refreshMsg.domainType() == EmaRdm.MMT_LOGIN && refreshMsg.state().streamState() == OmmState.StreamState.OPEN && refreshMsg.state().dataState() == OmmState.DataState.OK)	{
			// 3. Login accepted, app can post data now
			System.out.println("Login accepted, starting posting...");
			
			_postStreamID = refreshMsg.streamId();
			postMessage();
		}
		else	{
			System.out.println("Stream not open");
		}
	}
//	public void onLoginRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent event) {
//		System.out.println("----- Login Refresh message ----");
//		System.out.println(event.handle());
//
//		if(refreshMsg.state().streamState()==OmmState.StreamState.OPEN && refreshMsg.state().dataState()==OmmState.DataState.OK) {
//			if(_connectionUp==false) {
//				_connectionUp = true;
//				System.out.println("Connection Up");
//				//StartTunnelStream();
//			}
//			
//			
//		}else if (refreshMsg.state().streamState()==OmmState.StreamState.OPEN && refreshMsg.state().dataState()==OmmState.DataState.SUSPECT) {
//			_connectionUp = false;
//			_subItemOpen=false;
//			System.out.println("Connection Down");
//		}
//		
//	}
//	public void onLoginStatusMsg(StatusMsg statusMsg, OmmConsumerEvent event) {
//		System.out.println("----- Login Status message ----");
//		System.out.println(event.handle());
//
//		if(statusMsg.state().streamState()==OmmState.StreamState.OPEN && statusMsg.state().dataState()==OmmState.DataState.OK) {
//			if(_connectionUp==false) {
//				_connectionUp = true;
//				System.out.println("Connection Up");
//				//StartTunnelStream();
//			}
//		}else if (statusMsg.state().streamState()==OmmState.StreamState.OPEN && statusMsg.state().dataState()==OmmState.DataState.SUSPECT) {
//			_connectionUp = false;
//			System.out.println("Connection Down");
//			_subItemOpen=false;
//		}
//		
//	}
	public void onStatusMsg(StatusMsg statusMsg, OmmConsumerEvent event)	{
		System.out.println("----- Status message ----");
		System.out.println(statusMsg);
//		if(statusMsg.domainType()==EmaRdm.MMT_LOGIN && event.handle()==_loginStreamHandle) {
//			onLoginStatusMsg(statusMsg, event);
//		}
		if(event.handle()==_tunnelStreamHandle && statusMsg.domainType()==EmaRdm.MMT_SYSTEM && statusMsg.state().streamState()==OmmState.StreamState.CLOSED_RECOVER) {
			_tunnelStreamHandle = 0;
			_subItemOpen=false;
		}
		
		if(!_subItemOpen && event.handle() == _tunnelStreamHandle && statusMsg.state().streamState() == OmmState.StreamState.OPEN)	{
			// create a login request message
			ElementList elementList = EmaFactory.createElementList();
			elementList.add(EmaFactory.createElementEntry().ascii("Password", "RCC Password"));
			
			ReqMsg rMsg = EmaFactory.createReqMsg()
				.domainType(EmaRdm.MMT_LOGIN)
				.name("RCC Username")
				.attrib(elementList)
				// .privateStream(true)
				.serviceId(statusMsg.serviceId())
				.streamId(statusMsg.streamId());
			
			System.out.println("Sending client login request...");
			// get events from login substream	
			_subStreamHandle = _ommConsumer.registerClient(rMsg, this, 1, _tunnelStreamHandle);
			_subItemOpen = true;
		}
		
		
	}
	
	
	public void onAckMsg(AckMsg ackMsg, OmmConsumerEvent consumerEvent)	{
		System.out.println("----- Ack message ----");
		System.out.println(ackMsg);
		
		System.out.println("Continue posting...");
		//postMessage();
	}

	
	public void postMessage()	{
		try	{ Thread.sleep(300); } catch(Exception e) {}
		
		// populate the contributed FIDs and values 
		FieldList nestedFieldList = EmaFactory.createFieldList();
		nestedFieldList.add(EmaFactory.createFieldEntry().real(22, _bid++, OmmReal.MagnitudeType.EXPONENT_NEG_1));
		nestedFieldList.add(EmaFactory.createFieldEntry().real(25, _ask++, OmmReal.MagnitudeType.EXPONENT_NEG_1));
		
		// create an update message for our item
		UpdateMsg nestedUpdateMsg = EmaFactory.createUpdateMsg()
			.streamId(_postStreamID)
			.name("TRCCTEST01")
			.payload(nestedFieldList);
			
		// post this market price message
		_ommConsumer.submit(EmaFactory.createPostMsg()
			.streamId(_postStreamID)
			.postId(_postID++)
			.domainType(EmaRdm.MMT_MARKET_PRICE)
			.solicitAck(true)
			.complete(true)
			.payload(nestedUpdateMsg), _subStreamHandle);
	}

	
	public void setOmmConsumer(OmmConsumer ommConsumer)	{
		_ommConsumer = ommConsumer;
	}
	
	public void setTunnelHandle(long tunnelStreamHandle)	{
		_tunnelStreamHandle = tunnelStreamHandle;
	}

	public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent event)	{
		System.out.println("----- Update message ----");
		if(updateMsg.domainType() == EmaRdm.MMT_DIRECTORY) {
			onSourceDirectoyUpdateMsg( updateMsg,  event);
		}
	}

	public void onGenericMsg(GenericMsg genericMsg, OmmConsumerEvent consumerEvent)	{
		System.out.println("----- Generic message ----");
	}

	public void onAllMsg(Msg msg, OmmConsumerEvent consumerEvent)	{}
	
	private void StartTunnelStream() {
		ClassOfService cos = EmaFactory.createClassOfService()
				.authentication(EmaFactory.createCosAuthentication().type(CosAuthentication.CosAuthenticationType.NOT_REQUIRED))
				.dataIntegrity(EmaFactory.createCosDataIntegrity().type(CosDataIntegrity.CosDataIntegrityType.RELIABLE))
				.flowControl(EmaFactory.createCosFlowControl().type(CosFlowControl.CosFlowControlType.BIDIRECTIONAL).recvWindowSize(1200))
				.guarantee(EmaFactory.createCosGuarantee().type(CosGuarantee.CosGuaranteeType.NONE));

			System.out.println("Starting tunnel stream...");
			// Create a request for a tunnel stream
			TunnelStreamRequest tsr = EmaFactory.createTunnelStreamRequest()
				.classOfService(cos)
				.domainType(EmaRdm.MMT_SYSTEM)
				.name("TUNNEL1")
				.serviceName("DDS_TRCE");

			// Send the request and register for events from tunnel stream	
			long tunnelStreamHandle = _ommConsumer.registerClient(tsr, this);
			
			setTunnelHandle(tunnelStreamHandle);
	}
}


public class Contributor	{
	public static void main(String[] args)	{
		
		try		{
			System.out.println("Contributing to Refinitiv Contributions Channel");
			AppClient appClient = new AppClient();

			System.out.println("Starting encrypted connection...");
			// Create an OMM consumer
			OmmConsumer consumer = EmaFactory.createOmmConsumer(EmaFactory.createOmmConsumerConfig());
				//.tunnelingKeyStoreFile("KEYSTORE_FILE_NAME")
				//.tunnelingKeyStorePasswd("KEYSTORE_PASSWORD"));
			
			LoginReq loginReq = EmaFactory.Domain.createLoginReq();
			
			long loginhandle = consumer.registerClient(loginReq.message(), appClient);
			//appClient.setLoginHandle(loginhandle);
			
			ReqMsg reqMsg = EmaFactory.createReqMsg();
			consumer.registerClient(reqMsg.domainType(EmaRdm.MMT_DIRECTORY).serviceName("DDS_TRCE"), appClient);

//			ClassOfService cos = EmaFactory.createClassOfService()
//				.authentication(EmaFactory.createCosAuthentication().type(CosAuthentication.CosAuthenticationType.NOT_REQUIRED))
//				.dataIntegrity(EmaFactory.createCosDataIntegrity().type(CosDataIntegrity.CosDataIntegrityType.RELIABLE))
//				.flowControl(EmaFactory.createCosFlowControl().type(CosFlowControl.CosFlowControlType.BIDIRECTIONAL).recvWindowSize(1200))
//				.guarantee(EmaFactory.createCosGuarantee().type(CosGuarantee.CosGuaranteeType.NONE));
//
//			System.out.println("Starting tunnel stream...");
//			// Create a request for a tunnel stream
//			TunnelStreamRequest tsr = EmaFactory.createTunnelStreamRequest()
//				.classOfService(cos)
//				.domainType(EmaRdm.MMT_SYSTEM)
//				.name("TUNNEL1")
//				.serviceName("DDS_TRCE");
//
//			// Send the request and register for events from tunnel stream	
//			long tunnelStreamHandle = consumer.registerClient(tsr, appClient);
			appClient.setOmmConsumer(consumer);
			//appClient.setTunnelHandle(tunnelStreamHandle);
			while(true)
				Thread.sleep(60000);
		} 
		catch (Exception excp)	{
			System.out.println(excp.getMessage());
		} 
	}
}
