package jtool.sql.model;


/**
 * @author Geln Yang
 * @version 1.0
 */
public interface DataHolder {

  public int getSize();

  public RowHolder getRow(int i);
}
