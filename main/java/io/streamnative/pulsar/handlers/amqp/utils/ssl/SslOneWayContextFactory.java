package io.streamnative.pulsar.handlers.amqp.utils.ssl;

import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;

@Slf4j
public final class SslOneWayContextFactory {
	
	 private static final String PROTOCOL = "TLS";
	
	private static SSLContext SERVER_CONTEXT;//服务器安全套接字协议
	
    private static SSLContext CLIENT_CONTEXT;//客户端安全套接字协议
    
    
	public static SSLContext getServerContext(String pkPath,String keyPwd){
		if(SERVER_CONTEXT!=null) return SERVER_CONTEXT;
		InputStream in =null;
		InputStream tIN = null;
		// log.info"qqqq-----------"+pkPath+"---------------"+keyPwd);
		try{
			//密钥管理器
			KeyManagerFactory kmf = null;
			if(pkPath!=null){

				//密钥库KeyStore
				KeyStore ks = KeyStore.getInstance("JKS");
				//加载服务端证书
				in = new FileInputStream(pkPath);
				//加载服务端的KeyStore  ；sNetty是生成仓库时设置的密码，用于检查密钥库完整性的密码
				ks.load(in,keyPwd.toCharArray());
				
				kmf = KeyManagerFactory.getInstance("SunX509");
				//初始化密钥管理器
				kmf.init(ks,keyPwd.toCharArray());
			}
			TrustManagerFactory trustManagerFactory = null;
			if(pkPath!=null){
				KeyStore tks = KeyStore.getInstance("JKS");
				tIN = new FileInputStream(pkPath);
				tks.load(tIN, keyPwd.toCharArray());
				trustManagerFactory = TrustManagerFactory.getInstance("SunX509");
				trustManagerFactory.init(tks);
			}
			//获取安全套接字协议（TLS协议）的对象
			SERVER_CONTEXT= SSLContext.getInstance(PROTOCOL);
			//初始化此上下文
			//参数一：认证的密钥      参数二：对等信任认证  参数三：伪随机数生成器 。 由于单向认证，服务端不用验证客户端，所以第二个参数为null
			SERVER_CONTEXT.init(kmf.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

		}catch(Exception e){
			throw new Error("Failed to initialize the server-side SSLContext", e);
		}finally{
			if(in !=null){
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (tIN != null){
				try {
					tIN.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				tIN = null;
			}
		}
		return SERVER_CONTEXT;
	 }
	
	
	 public static SSLContext getClientContext(String caPath){
		 if(CLIENT_CONTEXT!=null) return CLIENT_CONTEXT;
		 
		 InputStream tIN = null;
		 try{
			 //信任库 
			TrustManagerFactory tf = null;
			if (caPath != null) {
				//密钥库KeyStore
			    KeyStore tks = KeyStore.getInstance("JKS");
			    //加载客户端证书
			    tIN = new FileInputStream(caPath);
			    tks.load(tIN, "nettyDemo".toCharArray());
			    tf = TrustManagerFactory.getInstance("SunX509");
			    // 初始化信任库  
			    tf.init(tks);
			}
			 
			 CLIENT_CONTEXT = SSLContext.getInstance(PROTOCOL);
			 //设置信任证书
			 CLIENT_CONTEXT.init(null,tf == null ? null : tf.getTrustManagers(), null);
			 
		 }catch(Exception e){
			 throw new Error("Failed to initialize the client-side SSLContext");
		 }finally{
			 if(tIN !=null){
					try {
						tIN.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
		 }
		 
		 return CLIENT_CONTEXT;
	 }

}
