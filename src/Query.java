import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import storageManager.*;

public class Query {
    private static Parser parser;
    private static MainMemory memory;
    private static Disk disk;
    private static SchemaManager schemaMG;

    public Query(){
        parser = null;
        memory = new MainMemory();
        disk = new Disk();
        schemaMG=new SchemaManager(memory, disk);
        disk.resetDiskIOs();
        disk.resetDiskTimer();
    }

    public void parseQuery(String m) throws Exception{
        String[] Command= m.trim().toLowerCase().split("\\s");
        String first = Command[0];
        parser = new Parser();
        parser.Parse(m);
        switch (first){
            case "create": this.createQuery(m);
                break;
            case "select": this.selectQuery(m);
                break;
            case "drop"  : this.dropQuery(m);
                break;
            case "delete": this.deleteQuery(m);
                break;
            case "insert": this.insertQuery(m);
                break;
            default: throw new Exception("Not a legal command!");
        }
    }

    /*
    public void CreateQuery(String m){
        ArrayList<String> field_names= parser.fields;
        ArrayList<FieldType> field_types= parser.fieldtypes;
        Schema schema=new Schema(field_names,field_types);
        //schemaMG.createRelation(parser.table_name,schema);
        Relation relation_reference=schemaMG.createRelation(parser.table_name,schema);
        System.out.print(relation_reference.getSchema() + "\n");

    }
    */
    private void createQuery(String sql){
        Statement stmt = parser.createStatement(sql);
        Schema schema = new Schema(stmt.fieldNames, stmt.fieldTypes);
        schemaMG.createRelation(stmt.tableName, schema);
    }


    public void selectQuery(String m){

    }

    private void dropQuery(String sql){
        Parser parser = new Parser();
        schemaMG.deleteRelation(parser.dropStatement(sql).trim());
    }

    public void deleteQuery(String m){

    }

    private void insertQuery(String sql){
        Statement stmt = parser.insertStatement(sql);
        Schema schema = schemaMG.getSchema(stmt.tableName);
        Relation relation = schemaMG.getRelation(stmt.tableName);
        //System.out.println(stmt.fieldValues.get(0).size());
        for(int i = 0; i < stmt.fieldValues.size(); ++i){
            Tuple tuple = relation.createTuple();
            for(int j = 0; j < stmt.fieldValues.get(0).size(); ++j ){
                String value = stmt.fieldValues.get(i).get(j);
                if(schema.getFieldType(j) == FieldType.INT){
                    tuple.setField(j, Integer.parseInt(value));
                }else{
                    tuple.setField(j, value);
                }
            }
            System.out.println(tuple.getField(0).integer);
            appendTuple2Relation(relation, tuple, 0);
        }
        System.out.println(relation.getNumOfTuples());
    }

    private static void appendTuple2Relation(Relation relation, Tuple tuple, int memBlockIndex){
        Block block;
        if(relation.getNumOfBlocks() != 0){
            relation.getBlock(relation.getNumOfBlocks() - 1, memBlockIndex);
            block = memory.getBlock(memBlockIndex);
            if(block.isFull()){
                block.clear();
                block.appendTuple(tuple);
                relation.setBlock(relation.getNumOfBlocks(), memBlockIndex);
            }else{
                block.appendTuple(tuple);
                relation.setBlock(relation.getNumOfBlocks() - 1, memBlockIndex);
            }
        }else{
            block = memory.getBlock(memBlockIndex);
            block.appendTuple(tuple);
            relation.setBlock(0, memBlockIndex);
        }
    }

    public static void main(String[] args) throws IOException {

        Query query = new Query();
        while(true) {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String s = br.readLine();
            s = s.toLowerCase();
            try {
                query.parseQuery(s);
            } catch (Exception e) {
                System.out.println("Got an Exception:" + e.getMessage());
                e.printStackTrace();
            }
            //br.close();
        }
    }

}
