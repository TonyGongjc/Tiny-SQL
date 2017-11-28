import javafx.util.Pair;
import storageManager.FieldType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Parser {
    public String table_name;
    public ArrayList<String> fields;
    public ArrayList<FieldType> fieldtypes;

    public Parser(){
        fields = new ArrayList<String>();
        fieldtypes = new ArrayList<FieldType>();

    }

    public boolean Parse(String m) throws Exception{
        String[] Command= m.trim().toLowerCase().split("\\s");
        String first = Command[0];
        switch (first){
            case "create": createStatement(m);
            break;
            case "select": selectStatement(m);
            break;
            case "drop"  : dropStatement(m);
            break;
            case "delete": deleteStatement(m);
            break;
            case "insert": insertStatement(m);
            break;
            default: throw new Exception("Not a legal command!");
        }
        return true;
    }

    /*
    public boolean CreateStatement(String m) throws Exception{
        if(checkCreate(m)) {
            String[] Command= m.trim().toLowerCase().split("\\s");
            table_name = Command[2];
            Pattern pattern = Pattern.compile("\\((.+)\\)");
            Matcher matcher = pattern.matcher(m);
            matcher.find();
            String[] values = matcher.group(1).trim().split("[\\s]*,[\\s]*");
            for(String v:values) {
                String[] field=v.toLowerCase().split(" ");
                String fieldT=field[1];
                FieldType type;
                if(fieldT.equals("str20")){
                    type=FieldType.STR20;
                }else if(fieldT.equals("int")){
                    type=FieldType.INT;
                }else{
                    throw new Exception("Wrong file type!!");
                }
                fields.add(field[0]);
                fieldtypes.add(type);
            }
            for(String s:this.fields){
                System.out.println(s);
            }
            return true;
        }else{
            throw new Exception("No legal values");
        }
    }

*/
    public Statement createStatement(String sql) {
        Statement stmt = new Statement();
        String regex = "create[\\s]+table[\\s]+(.+)[\\s]+\\((.*)\\)";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(sql);
        while(m.find()){
            stmt.tableName = m.group(1);
            String[] tmp = m.group(2).split("[\\s]*,[\\s]*");
            for(String s : tmp){
                stmt.fieldNames.add(s.split(" ")[0]);
                if(s.split("[\\s]+")[1].equals("int")){
                    stmt.fieldTypes.add(FieldType.INT);
                }else{
                    stmt.fieldTypes.add(FieldType.STR20);
                }
            }
        }
        /*
        for(FieldType s : stmt.fieldTypes){
            System.out.println(s);
        }
        */
        return stmt;
    }


    public Statement selectStatement(String m){
        String[] Command= m.trim().toLowerCase().replaceAll("[,\\s]+", " ").split("\\s");

        Statement stmt = new Statement();
        ParseTree parseTree = new ParseTree("select");

        for(int i=0; i<Command.length;i++) {
            String word=Command[i];
            //System.out.print(word);
            if (word.equals("distinct")) {
                parseTree.distinct=true;
                parseTree.distID=i;
            }
            if (word.equals("from")){
                parseTree.fromID=i;
            }
            if (word.equals("where")){
                parseTree.whereID=i;
                parseTree.where=true;
            }
            if(i<=Command.length-2){
                if (word.equals("order")&&Command[i+1].equals("by")){
                    parseTree.order=true;
                    parseTree.orderID=i;
                }
            }
        }
        if(parseTree.distinct) {
            parseTree.dist_attribute = Command[parseTree.distID + 1];
            parseTree.attributes = new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(Command, 2, parseTree.fromID)));
        }else{
            parseTree.attributes =  new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(Command, 1, parseTree.fromID)));
        }
        if(parseTree.where){
            parseTree.tables= new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(Command, parseTree.fromID+1, parseTree.whereID)));
            if(parseTree.order){
                String[] condition= Arrays.copyOfRange(Command, parseTree.whereID+1, parseTree.orderID);
                parseTree.expressionTree = new ExpressionTree(condition);
                parseTree.expressionTree.PrintTreeNode();
                parseTree.orderBy=Command[Command.length-1];
            }else{
                String[] condition=Arrays.copyOfRange(Command, parseTree.whereID+1,Command.length);
                parseTree.expressionTree= new ExpressionTree(condition);
              //  parseTree.expressionTree.PrintTreeNode();
            }
        }else{
            if(parseTree.order){
                parseTree.tables= new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(Command,parseTree.fromID+1, parseTree.orderID)));
                parseTree.orderBy= Command[Command.length-1];
            }else{
                parseTree.tables= new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(Command, parseTree.fromID+1, Command.length)));
            }
        }

        stmt.parseTree = parseTree;
        return stmt;
    }

    public String dropStatement(String sql){
        return sql.split("[\\s]+")[2];
    }

    public ParseTree deleteStatement(String m){
        System.out.println("I'm in Delete");
        String[] Command= m.trim().toLowerCase().replaceAll("[,\\s]+", " ").split("\\s");
        ParseTree parseTree = new ParseTree("delete");

        for(int i=0; i<Command.length;i++) {
            String word=Command[i];
            System.out.println(word);
            if (word.equals("where")){
                parseTree.whereID=i;
                parseTree.where=true;
            }
        }
        if(parseTree.where){
            parseTree.tables= new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(Command, parseTree.fromID+1, parseTree.whereID)));
                String[] condition=Arrays.copyOfRange(Command, parseTree.whereID+1,Command.length);
                parseTree.expressionTree= new ExpressionTree(condition);
                parseTree.expressionTree.PrintTreeNode();
        }else{
                parseTree.tables= new ArrayList<String>(Arrays.asList(Arrays.copyOfRange(Command, parseTree.fromID+1, Command.length)));
        }

        return parseTree;
    }

    public Statement insertStatement(String sql){
        Statement stmt = new Statement();
        String regex = "insert[\\s]+into[\\s]+(.+)[\\s]+\\((.*)\\)[\\s]+values[\\s]+(.*)";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(sql);
        if(m.find()){
            stmt.tableName = m.group(1);
            stmt.fieldNames = new ArrayList<>(Arrays.asList(m.group(2).split("[\\s]*,[\\s]*")));
            String[] values = m.group(3).replace("(", "").replace(")", "").split("[\\s]*,[\\s]*");
            int len = stmt.fieldNames.size();
            ArrayList<String> tmp = new ArrayList<>();
            for(int i = 0; i < values.length; ++i){
                if(i % len == 0) {
                    if(i != 0) stmt.fieldValues.add(tmp);
                    tmp.clear();
                }
                tmp.add(values[i]);
            }
            stmt.fieldValues.add(tmp);
        }else{
            System.out.println("Invalid SQL");
        }
        //System.out.println(stmt.fieldValues.get(0).size());
        return stmt;
    }

/*
    public boolean checkCreate(String m){
        String[] Command = m.trim().toLowerCase().split("\\s");
        if (Command.length <= 3) {
            System.out.println("Illegal Create");
            return false;
        }
        if (!Command[1].equalsIgnoreCase("table")) {
            System.out.println("Table should follow Create statement");
            return false;
        }

        if (!Command[3].startsWith("(") || !Command[Command.length - 1].endsWith(")")) {
            System.out.println("Miss parenthesizes");
            return false;
        }
        return true;
    }
    */

    public static void main(String[] args) throws IOException{
        BufferedReader br =new BufferedReader(new InputStreamReader(System.in));
        String s = br.readLine();

        Parser P=new Parser();
        try {
            P.Parse(s);
        }catch(Exception e){
            System.out.println("Got an Exception:"+e.getMessage());
            e.printStackTrace();
        }
        br.close();
    }
}

