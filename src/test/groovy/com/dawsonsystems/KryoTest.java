package com.dawsonsystems;

import com.dawsonsystems.session.KryoSerializer;
import org.apache.catalina.session.StandardSession;
import org.junit.Test;
import org.springframework.mock.web.MockHttpSession;
//import org.springframework.security.GrantedAuthority;
//import org.springframework.security.providers.UsernamePasswordAuthenticationToken;

import javax.servlet.http.HttpSession;
import java.io.IOException;

public class KryoTest {


    @Test
    public void testKryo() throws ClassNotFoundException, IOException {

//        KryoSerializer serializer = new KryoSerializer(getClass().getClassLoader());
//        MockHttpSession session = new MockHttpSession();
//
//        session.setAttribute("auth", new UsernamePasswordAuthenticationToken("Principal", "Auths", new GrantedAuthority[0]));
//
//        byte[] bytes = serializer.serializeFrom(session);
//
//        HttpSession newSession = serializer.deserializeInto(bytes, new MockHttpSession());

    }

}
