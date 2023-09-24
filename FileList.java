
/*******************************************************************************
 * @file  FileList.java
 *
 * @author   John Miller
 */

import java.io.*;
import static java.lang.System.out;
import java.util.*;

/*******************************************************************************
 * This class allows data tuples/tuples (e.g., those making up a relational table)
 * to be stored in a random access file.  This implementation requires that each
 * tuple be packed into a fixed length byte array.
 */
public class FileList
       extends AbstractList <Comparable []>
       implements List <Comparable []>, RandomAccess
{
    /** File extension for data files.
     */
    private static final String EXT = ".dat";

    /** The random access file that holds the tuples.
     */
    private RandomAccessFile file;

    /** The name of table.
     */
    private final String tableName;

    /** The number bytes required to store a "packed tuple"/record.
     */
    private final int recordSize;

    /** Counter for the number of tuples in this list.
     */
    private int nRecords = 0;

    /***************************************************************************
     * Construct a FileList.
     * @param _tableName   the name of the table
     * @param _recordSize  the size of tuple in bytes.
     */
    public FileList (String _tableName, int _recordSize)
    {
        tableName  = _tableName;
        recordSize = _recordSize;

        try {
            file = new RandomAccessFile (tableName + EXT, "rw");
        } catch (FileNotFoundException ex) {
            file = null;
            out.println ("FileList.constructor: unable to open - " + ex);
        } // try
    } // constructor

    /***************************************************************************
     * Add a new tuple into the file list by packing it into a record and writing
     * this record to the random access file.  Write the record either at the
     * end-of-file or into a empty slot.
     * @param tuple  the tuple to add
     * @return  whether the addition succeeded
     */
    public boolean add (Comparable [] tuple)
    {
        // changed the initialization to match the correct size of the record
        byte[] record = new byte[recordSize]; //FIX: table.pack (tuple);

        // TO BE IMPLEMENTED

        // Serialize and write each element of the tuple to the record
        for (int i = 0; i < tuple.length; i++) 
        {
            try 
            {
                ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
                objectStream.writeObject(tuple[i]);
                objectStream.flush();
                byte[] serializedData = byteStream.toByteArray();
                
                // Check if the serialized data size matches the expected size
                if (serializedData.length != 4) 
                {
                    out.println("FileList.add: wrong element size " + serializedData.length);
                    return false;
                }

                // Copy the serialized data to the record array
                System.arraycopy(serializedData, 0, record, i * 4, serializedData.length);
                
                objectStream.close();
            } 
            catch (IOException ex) 
            {
                out.println("FileList.add: unable to serialize tuple - " + ex);
                return false;
            }
        }
        try 
        {
            // Calculate the file position for the new record
            long filePosition = (long) nRecords * recordSize;

            // Seek to the correct position in the file
            file.seek(filePosition);

            // Write the record to the file
            file.write(record);

            // Increment the record count
            nRecords++;

            return true;
        } catch (IOException ex) {
            out.println("FileList.add: unable to write record - " + ex);
            return false;
        }

    } // add

    /***************************************************************************
     * Get the ith tuple by seeking to the correct file position and reading the
     * record.
     * @param i  the index of the tuple to get
     * @return  the ith tuple
     */
    public Comparable [] get (int i)
    {
        var record = new byte [recordSize];

        // TO BE IMPLEMENTED
        try {
            // Calculate the file position for the desired record
            long filePosition = (long) i * recordSize;
    
            // Seek to the correct position in the file
            file.seek(filePosition);
    
            // Read the record from the file into the 'record' byte array
            file.read(record);
    
            // Create a Comparable array to store the unpacked tuple
            Comparable[] tuple = new Comparable[record.length / 4]; // Assuming integers
    
            // Deserialize each element of the tuple
            for (int j = 0; j < tuple.length; j++) 
            {
                byte[] serializedData = Arrays.copyOfRange(record, j * 4, (j + 1) * 4); // Assuming integers
                ByteArrayInputStream byteStream = new ByteArrayInputStream(serializedData);
                ObjectInputStream objectStream = new ObjectInputStream(byteStream);
                tuple[j] = (Comparable) objectStream.readObject();
                objectStream.close();
            }
    
            return tuple;
        } 
        catch (IOException | ClassNotFoundException ex) 
        {
            out.println("FileList.get: unable to read record - " + ex);
            return null; // FIX: table.unpack (record);
        }
    } // get

    /***************************************************************************
     * Return the size of the file list in terms of the number of tuples/records.
     * @return  the number of tuples
     */
    public int size ()
    {
        return nRecords;
    } // size

    /***************************************************************************
     * Close the file.
     */
    public void close ()
    {
        try {
            file.close ();
        } catch (IOException ex) {
            out.println ("FileList.close: unable to close - " + ex);
        } // try
    } // close

} // FileList class

