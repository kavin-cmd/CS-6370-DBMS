
/****************************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 */

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;
import java.util.function.Predicate;

import static java.lang.Boolean.*;
import static java.lang.System.arraycopy;
import static java.lang.System.out;

/****************************************************************************************
 * The Table class implements relational database tables (including attribute names, domains
 * and a list of tuples.  Five basic relational algebra operators are provided: project,
 * select, union, minus and join.  The insert data manipulation operator is also provided.
 * Missing are update and delete data manipulation operators.
 */
public class Table
       implements Serializable
{
    /** Relative path for storage directory
     */
    private static final String DIR = "store" + File.separator;

    /** Filename extension for database files
     */
    private static final String EXT = ".dbf";

    /** Counter for naming temporary tables.
     */
    private static int count = 0;

    /** Table name.
     */
    private final String name;

    /** Array of attribute names.
     */
    private final String [] attribute;

    /** Array of attribute domains: a domain may be
     *  integer types: Long, Integer, Short, Byte
     *  real types: Double, Float
     *  string types: Character, String
     */
    private final Class [] domain;

    /** Collection of tuples (data storage).
     */
    private final List <Comparable []> tuples;

    /** Primary key (the attributes forming). 
     */
    private final String [] key;

    /** Index into tuples (maps key to tuple).
     */
    private final Map <KeyType, Comparable []> index;

    /** The supported map types.
     */
    private enum MapType { NO_MAP, TREE_MAP, LINHASH_MAP, BPTREE_MAP }

    /** The map type to be used for indices.  Change as needed.
     */
    private static final MapType mType = MapType.TREE_MAP;

    /************************************************************************************
     * Make a map (index) given the MapType.
     */

    // declaring the variable recordsize for filelist 
    private int recordSize;

    private FileList fileList;

    private static Map <KeyType, Comparable []> makeMap ()
    {
        return switch (mType) {
        case TREE_MAP    -> new TreeMap <> ();
        case LINHASH_MAP -> new LinHashMap <> (KeyType.class, Comparable [].class);
//      case BPTREE_MAP  -> new BpTreeMap <> (KeyType.class, Comparable [].class);
        default          -> null;
        }; // switch
    } // makeMap

    /************************************************************************************
     * Concatenate two arrays of type T to form a new wider array.
     *
     * @see http://stackoverflow.com/questions/80476/how-to-concatenate-two-arrays-in-java
     *
     * @param arr1  the first array
     * @param arr2  the second array
     * @return  a wider array containing all the values from arr1 and arr2
     */
    public static <T> T [] concat (T [] arr1, T [] arr2)
    {
        T [] result = Arrays.copyOf (arr1, arr1.length + arr2.length);
        arraycopy (arr2, 0, result, arr1.length, arr2.length);
        return result;
    } // concat

    //-----------------------------------------------------------------------------------
    // Constructors
    //-----------------------------------------------------------------------------------

    /************************************************************************************
     * Construct an empty table from the meta-data specifications.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = new ArrayList <> ();
        //tuples = new FileList(_name, count);
        index     = makeMap ();
    } // primary constructor

    /************************************************************************************
     * Construct a table from the meta-data specifications and data in _tuples list.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     * @param _tuples     the list of tuples containing the data
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key,
                  List <Comparable []> _tuples)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = _tuples;
        index     = makeMap ();
        
        //comment/uncomment starts from here to execute the files based on arrayList()/ FileList
        // Calculate recordSize
        recordSize = 0;
        for (Class<?> aDomain : domain) 
        {
            if (aDomain == Integer.class) 
            {
                recordSize += 4; // Integer takes 4 bytes
            } 
            else if (aDomain == String.class) 
            {
                recordSize += 64; // Assume a maximum string size of 64 bytes
            } 
        }

        // Initialize the FileList
        FileList fileList = new FileList(_name, recordSize);
        for (Comparable[] tuple : tuples) 
        {
            fileList.add(tuple);
        }
        // comment/ uncomment ends here to execute the ArrayList part of the output
    }



    /************************************************************************************
     * Construct an empty table from the raw string specifications.
     *
     * @param _name       the name of the relation
     * @param attributes  the string containing attributes names
     * @param domains     the string containing attribute domains (data types)
     * @param _key        the primary key
     */
    public Table (String _name, String attributes, String domains, String _key)
    {
        this (_name, attributes.split (" "), findClass (domains.split (" ")), _key.split(" "));

        out.println ("DDL> create table " + name + " (" + attributes + ")");
    } // constructor

    //----------------------------------------------------------------------------------
    // Public Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Project the tuples onto a lower dimension by keeping only the given attributes.
     * Check whether the original key is included in the projection.
     *
     * #usage movie.project ("title year studioNo")
     *
     * @param attributes  the attributes to project onto
     * @return  a table of projected tuples
     */
    public Table project (String attributes)
    {
        out.println ("RA> " + name + ".project (" + attributes + ")");
        var attrs     = attributes.split (" ");
        var colDomain = extractDom (match (attrs), domain);
        var newKey    = (Arrays.asList (attrs).containsAll (Arrays.asList (key))) ? key : attrs;
        
        //switch between the default and the fileList execution 

        // List <Comparable []> rows = new ArrayList <> ();
        List <Comparable []> rows = new FileList(attributes, recordSize);

        //  T O   B E   I M P L E M E N T E D 
        for (var tuple : tuples) 
        {
            Comparable[] newRow = new Comparable[attrs.length];
            for (int i = 0; i < attrs.length; i++) 
            {
                int colIndex = col(attrs[i]);
                newRow[i] = tuple[colIndex];
            }
            rows.add(newRow);
        }

        return new Table (name + count++, attrs, colDomain, newKey, rows);
    } // project

    /************************************************************************************
     * Select the tuples satisfying the given predicate (Boolean function).
     *
     * #usage movie.select (t -> t[movie.col("year")].equals (1977))
     *
     * @param predicate  the check condition for tuples
     * @return  a table with tuples satisfying the predicate
     */
    public Table select (Predicate <Comparable []> predicate)
    {
        out.println ("RA> " + name + ".select (" + predicate + ")");
        
        return new Table (name + count++, attribute, domain, key,
                   tuples.stream ().filter (t -> predicate.test (t))
                                   .collect (Collectors.toList ()));
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given simple condition on attributes/constants
     * compared using an <op> ==, !=, <, <=, >, >=.
     *
     * #usage movie.select ("year == 1977")
     *
     * @param condition  the check condition as a string for tuples
     * @return  a table with tuples satisfying the condition
     */
    public Table select (String condition)
    {
        out.println ("RA> " + name + ".select (" + condition + ")");

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D
        for(var t:tuples)
        {
            if (t[col(condition)]!= null)
                rows.add(t);    
        }
        return new Table (name + count++, attribute, domain, key, rows);
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (key = value).  Use an index
     * (Map) to retrieve the tuple with the given key value.  INDEXED SELECT ALGORITHM.
     *
     * @param keyVal  the given key value
     * @return  a table with the tuple satisfying the key predicate
     */
    public Table select (KeyType keyVal)
    {
        out.println ("RA> " + name + ".select (" + keyVal + ")");

        List <Comparable []> rows = new ArrayList<>();

        // //  T O   B E   I M P L E M E N T E D  - Project 2
        // Check if the index is available
        if (mType != MapType.NO_MAP) 
        {
            // Use the index to efficiently retrieve the tuple with the given keyVal
            Comparable[] tuple = index.get(keyVal);

            // Check if the tuple with the keyVal exists in the table
            if (tuple != null) 
            {
                rows.add(tuple);
            }
        }

        // Create a new table with the selected rows
        return new Table(name + count++, attribute, domain, key, rows);
    } // select

    /************************************************************************************
     * Union this table and table2.  Check that the two tables are compatible.
     *
     * #usage movie.union (show)
     *
     * @param table2  the rhs table in the union operation
     * @return  a table representing the union
     */
    public Table union (Table table2)
    {
        out.println ("RA> " + name + ".union (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D 
        rows.addAll(tuples); 
        for (KeyType r : table2.index.keySet()) 
        {
            if (!index.containsKey(r))
            {
                rows.add(table2.index.get(r)); 
            } 
        }
        return new Table (name + count++, attribute, domain, key, rows);
    } // union
    /************************************************************************************
     * Take the difference of this table and table2.  Check that the two tables are
     * compatible.
     *
     * #usage movie.minus (show)
     *
     * @param table2  The rhs table in the minus operation
     * @return  a table representing the difference
     */
    public Table minus (Table table2)
    {
        out.println ("RA> " + name + ".minus (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D 
        for(int i = 0; i < this.tuples.size(); i++) 
        { 
            Comparable [] tuple = this.tuples.get(i);
            boolean toAdd = true; 
            for(int j = 0; j < table2.tuples.size(); j++) 
            { 
                Comparable [] tuple2 = table2.tuples.get(j); 
                if(Arrays.deepEquals(tuple, tuple2)) 
                { 
                    toAdd = false; 
                }
            }
            if(toAdd)
                rows.add(tuple);
        }
        return new Table (name + count++, attribute, domain, key, rows);
    } // minus

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Tuples from both tables
     * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
     * names by appending "2" to the end of any duplicate attribute name.  Implement using
     * a NESTED LOOP JOIN ALGORITHM.
     *
     * #usage movie.join ("studioName", "name", studio)
     *
     * @param attributes1  the attributes of this table to be compared (Foreign Key)
     * @param attributes2  the attributes of table2 to be compared (Primary Key)
     * @param table2       the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (String attributes1, String attributes2, Table table2)
    {
        out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", "
                                               + table2.name + ")");

        var t_attrs = attributes1.split (" ");
        var u_attrs = attributes2.split (" ");
        var rows    = new ArrayList <Comparable []> ();

        //  T O   B E   I M P L E M E N T E D 
        // Iterate through the tuples of the first table (this)
        for (Comparable[] thisTuple : tuples) 
        {
            // Iterate through the tuples of the second table (table2)
            for (Comparable[] table2Tuple : table2.tuples) 
            {
                // Initialize an array to hold the joined tuple
                Comparable[] joinedTuple = new Comparable[thisTuple.length + table2Tuple.length];

                // Copy attributes from the first table (this)
                for (int i = 0; i < thisTuple.length; i++) 
                {
                    joinedTuple[i] = thisTuple[i];
                }

                // Copy attributes from the second table (table2)
                for (int j = 0; j < table2Tuple.length; j++) 
                {
                    joinedTuple[thisTuple.length + j] = table2Tuple[j];
                }

                // Add the joined tuple to the list of rows
                rows.add(joinedTuple);
            }
        }

        return new Table (name + count++, concat (attribute, table2.attribute),concat (domain, table2.domain), key, rows);
    } // join

    /************************************************************************************
     * Join this table and table2 by performing a "theta-join".  Tuples from both tables
     * are compared attribute1 <op> attribute2.  Disambiguate attribute names by appending "2"
     * to the end of any duplicate attribute name.  Implement using a Nested Loop Join algorithm.
     *
     * #usage movie.join ("studioName == name", studio)
     *
     * @param condition  the theta join condition
     * @param table2     the rhs table in the join operation
     * @return  a table with tuples satisfying the condition
     */
    public Table join (String condition, Table table2)
    {
        out.println ("RA> " + name + ".join (" + condition + ", " + table2.name + ")");

        var rows = new ArrayList <Comparable []> ();
        //  T O   B E   I M P L E M E N T E D
        // Parse the condition into its components (attribute names and comparison operator)
    String[] conditionComponents = condition.split(" ");
    String attr1Name = conditionComponents[0];
    String op = conditionComponents[1];
    String attr2Name = conditionComponents[2];

    // Find the column positions for the attributes in the condition
    int attr1Col = col(attr1Name);
    int attr2Col = table2.col(attr2Name);

    // Check if both columns exist in their respective tables
    if (attr1Col == -1 || attr2Col == -1) {
        out.println("join: Attribute not found in one of the tables.");
        return null; // Handle the error gracefully
    }

    // Iterate through the tuples of the first table (this)
    for (Comparable[] thisTuple : tuples) 
    {
        // Iterate through the tuples of the second table (table2)
        for (Comparable[] table2Tuple : table2.tuples) 
        {
            // Evaluate the condition based on the specified operator
            boolean conditionMet = false;

            // Perform the comparison based on the operator
            if (op.equals("==")) {
                conditionMet = thisTuple[attr1Col].equals(table2Tuple[attr2Col]);
            } else if (op.equals("!=")) {
                conditionMet = !thisTuple[attr1Col].equals(table2Tuple[attr2Col]);
            } else {
                out.println("join: Unsupported operator: " + op);
                return null; // Handle unsupported operators gracefully
            }

            // If the condition is met, add the joined tuple to the result
            if (conditionMet) {
                Comparable[] joinedTuple = concat(thisTuple, table2Tuple);
                rows.add(joinedTuple);
            }
        }
    }
    return new Table(name + count++, concat(attribute, table2.attribute),
    concat(domain, table2.domain), key, rows);
} // join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above equi-join,
     * but implemented using an INDEXED JOIN ALGORITHM.
     *
     * @param attributes1  the attributes of this table to be compared (Foreign Key)
     * @param attributes2  the attributes of table2 to be compared (Primary Key)
     * @param table2       the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table i_join (String attributes1, String attributes2, Table table2)
    {
        //  T O   B E   I M P L E M E N T E D  - Project 2
        out.println("RA> " + name + ".i_join (" + attributes1 + ", " + attributes2 + "," + table2.name + ")");

        String[] u_attrs = attributes1.split(" ");
        String[] t_attrs = attributes2.split(" ");
    
        List<Comparable[]> rows = new ArrayList<>();
    
        // Check if the attributes match
        if (u_attrs.length != t_attrs.length) 
        {
            out.println("Please use attributes that are equivalent");
            return null;
        }
        
        // Check if the index is available for both tables
        if (mType != MapType.NO_MAP && table2.mType != MapType.NO_MAP) 
        {
            for (int i = 0; i < u_attrs.length; i++) 
            {
                Comparable[] ttup = tuples.get(i);
                KeyType keyVal = new KeyType(extract(ttup, u_attrs));
                Comparable[] utup = table2.index.get(keyVal);
                
                if (utup != null) 
                {
                    // A matching tuple is found in the index of table2
                    Comparable[] concatenatedTuple = new Comparable[ttup.length + utup.length];
                    System.arraycopy(ttup, 0, concatenatedTuple, 0, ttup.length);
                    System.arraycopy(utup, 0, concatenatedTuple, ttup.length, utup.length);
                    rows.add(concatenatedTuple);
                }
            }
        }
        
        return new Table(name + count++, attribute, domain, key, rows);
        //return null;

    } // i_join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above, but implemented
     * using a Hash Join algorithm.
     *
     * @param attributes1  the attributes of this table to be compared (Foreign Key)
     * @param attributes2  the attributes of table2 to be compared (Primary Key)
     * @param table2       the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table h_join (String attributes1, String attributes2, Table table2)
    {

        //  D O   N O T   I M P L E M E N T

        return null;
    } // h_join

    /************************************************************************************
     * Join this table and table2 by performing an "natural join".  Tuples from both tables
     * are compared requiring common attributes to be equal.  The duplicate column is also
     * eliminated.
     *
     * #usage movieStar.join (starsIn)
     *
     * @param table2  the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (Table table2)
    {
        out.println ("RA> " + name + ".join (" + table2.name + ")");

        var rows = new ArrayList <Comparable []> ();

        //  T O   B E   I M P L E M E N T E D 
        // Find the common attribute names between the two tables
        List<String> commonAttributes = new ArrayList<>();
        for (String attr1 : attribute) 
        {
            for (String attr2 : table2.attribute) 
            {
                if (attr1.equals(attr2)) 
                {
                    commonAttributes.add(attr1);
                    break;
                }
            }
        }

        // Check if there are no common attributes for the join
        if (commonAttributes.isEmpty()) 
        {
            out.println("join: No common attributes found for the join.");
            return null; 
        }

        // Create a mapping of attribute names to column positions for both tables
        Map<String, Integer> attrToColMap1 = new HashMap<>();
        Map<String, Integer> attrToColMap2 = new HashMap<>();
        for (int i = 0; i < attribute.length; i++) 
        {
            attrToColMap1.put(attribute[i], i);
        }
        for (int i = 0; i < table2.attribute.length; i++) 
        {
            attrToColMap2.put(table2.attribute[i], i);
        }

        // Iterate through the tuples of the first table (this)
        for (Comparable[] thisTuple : tuples) 
        {
            // Iterate through the tuples of the second table (table2)
            for (Comparable[] table2Tuple : table2.tuples) 
            {
                boolean joinable = true;
                Comparable[] joinedTuple = new Comparable[attribute.length + table2.attribute.length];

                // Copy attributes from the first table (this)
                for (int i = 0; i < attribute.length; i++) 
                {
                    int colPos = attrToColMap1.get(attribute[i]);
                    joinedTuple[i] = thisTuple[colPos];
                }

                // Copy attributes from the second table (table2)
                for (int i = 0; i < table2.attribute.length; i++) 
                {
                    int colPos = attrToColMap2.get(table2.attribute[i]);
                    joinedTuple[attribute.length + i] = table2Tuple[colPos];
                }

                // Check if the common attribute values match
                for (String commonAttr : commonAttributes) 
                {
                    int colPos1 = attrToColMap1.get(commonAttr);
                    int colPos2 = attrToColMap2.get(commonAttr);

                    if (!thisTuple[colPos1].equals(table2Tuple[colPos2])) 
                    {
                        joinable = false;
                        break;
                    }
                }

                // If the common attributes match, add the joined tuple to the result
                if (joinable) 
                {
                    rows.add(joinedTuple);
                }
            }
        }    
        // FIX - eliminate duplicate columns
        return new Table (name + count++, concat (attribute, table2.attribute), 
                                          concat (domain, table2.domain), key, rows);
    } // join

    /************************************************************************************
     * Return the column position for the given attribute name or -1 if not found.
     *
     * @param attr  the given attribute name
     * @return  a column position
     */
    public int col (String attr)
    {
        for (var i = 0; i < attribute.length; i++) {
           if (attr.equals (attribute [i])) return i;
        } // for

        return -1;       // -1 => not found
    } // col

    /************************************************************************************
     * Insert a tuple to the table.
     *
     * #usage movie.insert ("Star_Wars", 1977, 124, "T", "Fox", 12345)
     *
     * @param tup  the array of attribute values forming the tuple
     * @return  whether insertion was successful
     */
    public boolean insert (Comparable [] tup)
    {
        out.println ("DML> insert into " + name + " values ( " + Arrays.toString (tup) + " )");

        if (typeCheck (tup)) {
            tuples.add (tup);
            var keyVal = new Comparable [key.length];
            var cols   = match (key);
            for (var j = 0; j < keyVal.length; j++) keyVal [j] = tup [cols [j]];
            if (mType != MapType.NO_MAP) index.put (new KeyType (keyVal), tup);
            return true;
        } else {
            return false;
        } // if
    } // insert

    /************************************************************************************
     * Get the name of the table.
     *
     * @return  the table's name
     */
    public String getName ()
    {
        return name;
    } // getName

    /************************************************************************************
     * Print this table.
     */
    public void print ()
    {
        out.println ("\n Table " + name);
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
        out.print ("| ");
        for (var a : attribute) out.printf ("%15s", a);
        out.println (" |");
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
        for (var tup : tuples) {
            out.print ("| ");
            for (var attr : tup) out.printf ("%15s", attr);
            out.println (" |");
        } // for
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
    } // print

    /************************************************************************************
     * Print this table's index (Map).
     */
    public void printIndex ()
    {
        out.println ("\n Index for " + name);
        out.println ("-------------------");
        if (mType != MapType.NO_MAP) {
            for (var e : index.entrySet ()) {
                out.println (e.getKey () + " -> " + Arrays.toString (e.getValue ()));
            } // for
        } // if
        out.println ("-------------------");
    } // printIndex

    /************************************************************************************
     * Load the table with the given name into memory. 
     *
     * @param name  the name of the table to load
     */
    public static Table load (String name)
    {
        Table tab = null;
        try {
            ObjectInputStream ois = new ObjectInputStream (new FileInputStream (DIR + name + EXT));
            tab = (Table) ois.readObject ();
            ois.close ();
        } catch (IOException ex) {
            out.println ("load: IO Exception");
            ex.printStackTrace ();
        } catch (ClassNotFoundException ex) {
            out.println ("load: Class Not Found Exception");
            ex.printStackTrace ();
        } // try
        return tab;
    } // load

    /************************************************************************************
     * Save this table in a file.
     */
    public void save ()
    {
        try {
            var oos = new ObjectOutputStream (new FileOutputStream (DIR + name + EXT));
            oos.writeObject (this);
            oos.close ();
        } catch (IOException ex) {
            out.println ("save: IO Exception");
            ex.printStackTrace ();
        } // try
    } // save

    //----------------------------------------------------------------------------------
    // Private Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Determine whether the two tables (this and table2) are compatible, i.e., have
     * the same number of attributes each with the same corresponding domain.
     *
     * @param table2  the rhs table
     * @return  whether the two tables are compatible
     */
    private boolean compatible (Table table2)
    {
        if (domain.length != table2.domain.length) 
        {
            out.println ("compatible ERROR: table have different arity");
            return false;
        } // if   
        for (var j = 0; j < domain.length; j++) {
            if (domain [j] != table2.domain [j]) {
                out.println ("compatible ERROR: tables disagree on domain " + j);
                return false;
            } // if
        } // for
        return true;
    } // compatible

    /************************************************************************************
     * Match the column and attribute names to determine the domains.
     *
     * @param column  the array of column names
     * @return  an array of column index positions
     */
    private int [] match (String [] column)
    {
        int [] colPos = new int [column.length];

        for (var j = 0; j < column.length; j++) {
            var matched = false;
            for (var k = 0; k < attribute.length; k++) {
                if (column [j].equals (attribute [k])) {
                    matched = true;
                    colPos [j] = k;
                } // for
            } // for
            if ( ! matched) {
                out.println ("match: domain not found for " + column [j]);
            } // if
        } // for

        return colPos;
    } // match

    /************************************************************************************
     * Extract the attributes specified by the column array from tuple t.
     *
     * @param t       the tuple to extract from
     * @param column  the array of column names
     * @return  a smaller tuple extracted from tuple t 
     */
    private Comparable [] extract (Comparable [] t, String [] column)
    {
        var tup    = new Comparable [column.length];
        var colPos = match (column);
        for (var j = 0; j < column.length; j++) tup [j] = t [colPos [j]];
        return tup;
    } // extract

    /************************************************************************************
     * Check the size of the tuple (number of elements in array) as well as the type of
     * each value to ensure it is from the right domain. 
     *
     * @param t  the tuple as a array of attribute values
     * @return  whether the tuple has the right size and values that comply
     *          with the given domains
     */
    private boolean typeCheck (Comparable [] t)
    { 
        //  T O   B E   I M P L E M E N T E D 
        //check if tuple length matches attribute length
        if(t.length!=attribute.length)
        {
            out.println ("Typecheck error: The sizes do not match");
            return false;
        }
        //Iterating through every tuple in the table
        for(int i =0;i<t.length;i++)
        {
            //checking for null values
            if(t[i]==null)
            {
                out.println("Typecheck error: Null values are not allowed");
                return false;
            }
            // Check if the attribute value's type matches the expected domain type
            if (!domain[i].isInstance(t[i])) 
            {
                return false;
            }
        }
        return true;      
    } // typeCheck

    /************************************************************************************
     * Find the classes in the "java.lang" package with given names.
     *
     * @param className  the array of class name (e.g., {"Integer", "String"})
     * @return  an array of Java classes
     */
    private static Class [] findClass (String [] className)
    {
        var classArray = new Class [className.length];

        for (var i = 0; i < className.length; i++) {
            try {
                classArray [i] = Class.forName ("java.lang." + className [i]);
            } catch (ClassNotFoundException ex) {
                out.println ("findClass: " + ex);
            } // try
        } // for

        return classArray;
    } // findClass

    /************************************************************************************
     * Extract the corresponding domains.
     *
     * @param colPos  the column positions to extract.
     * @param group   where to extract from
     * @return  the extracted domains
     */
    private Class [] extractDom (int [] colPos, Class [] group)
    {
        var obj = new Class [colPos.length];

        for (var j = 0; j < colPos.length; j++) {
            obj [j] = group [colPos [j]];
        } // for

        return obj;
    } // extractDom

} // Table class

