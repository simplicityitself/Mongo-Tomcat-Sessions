package com.dawsonsystems.session;

import org.apache.catalina.session.StandardSession;
import org.apache.catalina.util.CustomObjectInputStream;

import javax.servlet.http.HttpSession;
import java.io.*;


public class JavaSerializer implements Serializer {
  private ClassLoader loader;

  @Override
  public void setClassLoader(ClassLoader loader) {
    this.loader = loader;
  }

  @Override
  public byte[] serializeFrom(HttpSession session) throws IOException {

    StandardSession standardSession = (StandardSession) session;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos));
    oos.writeLong(standardSession.getCreationTime());
    standardSession.writeObjectData(oos);

    oos.close();

    return bos.toByteArray();
  }

  @Override
  public HttpSession deserializeInto(byte[] data, HttpSession session) throws IOException, ClassNotFoundException {

    StandardSession standardSession = (StandardSession) session;

    BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(data));

    ObjectInputStream ois = new CustomObjectInputStream(bis, loader);
    standardSession.setCreationTime(ois.readLong());
    standardSession.readObjectData(ois);

    return session;
  }
}
