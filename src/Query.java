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
            case "select": this.SelectQuery(m);
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

/*
    private void selectQuery(String sql){
        Statement stmt = parser.selectStatement(sql);
        ArrayList<Tuple> res = new ArrayList<>();
        ParseTree parseTree = stmt.parseTree;
        String tableName = parseTree.tables.get(0);
        //Schema schema = schemaMG.getSchema(tableName);
        Relation relation = schemaMG.getRelation(tableName);
        Block block;
        for(int i = 0; i < relation.getNumOfBlocks(); ++i){
            relation.getBlock(i, 0);
            block  = memory.getBlock(0);
            ArrayList<Tuple> tuples = block.getTuples();
            for(Tuple tuple : tuples){
                if(parseTree.where){
                    parseTree.expressionTree.checkTuple(tuple);
                }else{
                    System.out.println(tuple);
                    res.add(tuple);
                }
            }
        }
    }
    */

    private void SelectQuery(String sql){
        Statement stmt = parser.selectStatement(sql);
        ArrayList<Tuple> res = new ArrayList<>();
        ParseTree parseTree = stmt.parseTree;
        String tableName = parseTree.tables.get(0);
        Relation relation = schemaMG.getRelation(tableName);
        int RelationNumber=relation.getNumOfBlocks();
        int MemoryNumber=memory.getMemorySize();

        if(RelationNumber<=MemoryNumber){
            res=selectQueryHelper(parseTree, relation, 0, 0,RelationNumber);
        }else{
            int remainNumber=RelationNumber;
            int relationindex=0;
            while(remainNumber>MemoryNumber){
                ArrayList<Tuple> result=selectQueryHelper(parseTree, relation, relationindex,0,MemoryNumber);
                res.addAll(result);
                remainNumber=remainNumber-MemoryNumber;
                relationindex=relationindex+MemoryNumber;
            }
            ArrayList<Tuple> result=selectQueryHelper(parseTree, relation, relationindex, 0, remainNumber);
            res.addAll(result);
        }
    }

    private ArrayList<Tuple> selectQueryHelper(ParseTree parseTree, Relation relation, int relationIndex, int memoryIndex, int loop ){
        Block block;
        ArrayList<Tuple> res = new ArrayList<>();
        relation.getBlocks(relationIndex, memoryIndex, loop);
        for(int i=0; i<loop; i++){
            block =memory.getBlock(i);
            ArrayList<Tuple> tuples = block.getTuples();
            for(Tuple tuple : tuples){
                if(parseTree.where){
                    parseTree.expressionTree.checkTuple(tuple);
                }else{
                    System.out.println(tuple);
                    res.add(tuple);
                }
            }
        }
        return res;
    }

    private void dropQuery(String sql){
        Parser parser = new Parser();
        schemaMG.deleteRelation(parser.dropStatement(sql).trim());
    }

    public void deleteQuery(String m){
        ParseTree parseTree=parser.deleteStatement(m);
        String tableName=parseTree.tables.get(0);
        deleteTuples(tableName, parseTree);

    }

    private void deleteTuples(String tableName, ParseTree parseTree){
        Relation relation=schemaMG.getRelation(tableName);
        int RelationBlock=relation.getNumOfBlocks();
        int MemoryBlock=memory.getMemorySize();

        if(RelationBlock<MemoryBlock){
            for(int i=0; i<RelationBlock; i++){
                //relation.getBlocks();
            }
        }
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
