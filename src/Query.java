import java.io.IOException;
import java.util.ArrayList;
import storageManager.*;

public class Query {
    private static Parser parser = new Parser();
    private static MainMemory memory = new MainMemory();
    private static Disk disk = new Disk();
    private static SchemaManager schemaMG = new SchemaManager(memory, disk);

    public SchemaManager getSchemaMG() {
        return schemaMG;
    }

    public Query(){ }

    private static void reset(){
        parser = new Parser();
        memory = new MainMemory();
        disk = new Disk();
        schemaMG = new SchemaManager(memory, disk);
        disk.resetDiskIOs();
        disk.resetDiskTimer();
    }

    public static void parseQuery(String m){
        m = m.toLowerCase();
        String[] Command= m.trim().toLowerCase().split("\\s");
        String first = Command[0];
        switch (first){
            case "create": createQuery(m);
                break;
            case "select": selectQuery(m);
                break;
            case "drop"  : dropQuery(m);
                break;
            case "delete": deleteQuery(m);
                break;
            case "insert": insertQuery(m);
                break;
            default: System.out.println("Not a legal command!");
        }
    }

    private static void createQuery(String sql){
        Statement stmt = parser.createStatement(sql);
        Schema schema = new Schema(stmt.fieldNames, stmt.fieldTypes);
        schemaMG.createRelation(stmt.tableName, schema);
    }

    private static void selectQuery(String sql){
        Statement stmt = parser.selectStatement(sql);
        ParseTree parseTree = stmt.parseTree;

        if(parseTree.tables.size() == 1){
            selectSingleRelation(parseTree);
        }else{
            selectMultiRelation(parseTree);
        }
    }

    private static void selectSingleRelation(ParseTree parseTree){
        String tableName = parseTree.tables.get(0);
        Relation relation = schemaMG.getRelation(tableName);
        ArrayList<String> tempRelations = new ArrayList<>();

        //if DISTINCT
        if(parseTree.distinct){
            relation = QueryHelper.distinct(schemaMG, relation, memory, parseTree.dist_attribute);
            QueryHelper.clearMainMem(memory);
            tempRelations.add(relation.getRelationName());
        }

        //selection
        if(parseTree.where){
            relation = QueryHelper.select(schemaMG, relation, memory, parseTree);
            QueryHelper.clearMainMem(memory);
            tempRelations.add(relation.getRelationName());
        }

        //if ORDER BY
        if(parseTree.order){
            relation = QueryHelper.sort(schemaMG, relation, memory, parseTree.orderBy);
            QueryHelper.clearMainMem(memory);
            tempRelations.add(relation.getRelationName());
        }

        //projection
        QueryHelper.project(relation, memory, parseTree);

        //delete all intermediate relations
        if(tempRelations.isEmpty()) return;
        for(String temp : tempRelations){
            if(schemaMG.relationExists(temp)) schemaMG.deleteRelation(temp);
        }
    }


    private static void selectMultiRelation(ParseTree parseTree){
        Relation relation = null;
        ArrayList<String> tempRelations = new ArrayList<>();
        if(parseTree.tables.size()==2) {
            if(parseTree.expressionTree.natureJoin.size() > 0){
                Pair<ArrayList<String>, String> condition = parseTree.expressionTree.natureJoin.get(0);
                ArrayList<String> joinInfo = multipleSelectHelper(condition);
                relation = Join.naturalJoin(schemaMG, memory, joinInfo.get(0), joinInfo.get(1), joinInfo.get(2));
            }else {
                relation = Join.crossProduct(schemaMG, memory, parseTree.tables.get(0), parseTree.tables.get(1));
            }
        }else {
            if(parseTree.expressionTree.natureJoin.size() !=0){
                for(int i=0; i<2; i++){
                    Pair<ArrayList<String>, String> condition = parseTree.expressionTree.natureJoin.get(i);
                    ArrayList<String> joinInfo = multipleSelectHelper(condition);
                    if(i==0) {
                        relation = Join.naturalJoin(schemaMG, memory, joinInfo.get(0), joinInfo.get(1), joinInfo.get(2));
                       // break;
                    }else{

                        relation = Join.crossProduct(schemaMG, memory, relation.getRelationName(), joinInfo.get(1));
                    }
                }
            }else{
                for(int i=0; i<parseTree.tables.size()-1; i++){
                    if(i==0){
                        relation = Join.crossProduct(schemaMG, memory, parseTree.tables.get(0), parseTree.tables.get(1));
                    }else{
                        relation = Join.crossProduct(schemaMG, memory, relation.getRelationName(), parseTree.tables.get(i+1));
                    }
                }
            }
        }
        tempRelations.add(relation.getRelationName());


        //if DISTINCT
        if(parseTree.distinct){
            relation = QueryHelper.distinct(schemaMG, relation, memory, parseTree.dist_attribute);
            QueryHelper.clearMainMem(memory);
            tempRelations.add(relation.getRelationName());
        }

        //selection
        if(parseTree.where){
            relation = QueryHelper.select(schemaMG, relation, memory, parseTree);
            QueryHelper.clearMainMem(memory);
            tempRelations.add(relation.getRelationName());
        }

        //if ORDER BY
        if(parseTree.order){
            relation = QueryHelper.sort(schemaMG, relation, memory, parseTree.orderBy);
            QueryHelper.clearMainMem(memory);
            tempRelations.add(relation.getRelationName());
        }

        //projection
        System.out.println(relation.getNumOfTuples());
        QueryHelper.project(relation, memory, parseTree);

        System.out.println(relation.getRelationName());
        ArrayList<String> names = relation.getSchema().getFieldNames();
        for(String name:names){
            System.out.println(name);
        }
        if(tempRelations.isEmpty()) return;
        for(String temp : tempRelations){
            if(schemaMG.relationExists(temp)) schemaMG.deleteRelation(temp);
        }
    }

    private static ArrayList<String> multipleSelectHelper(Pair<ArrayList<String>, String> condition){
        ArrayList<String>joinInfo = new ArrayList<String>();
        joinInfo.add(condition.first.get(0));
        joinInfo.add(condition.first.get(1));
        joinInfo.add(condition.second);
        return joinInfo;
    }

    private static void dropQuery(String sql){
        Parser parser = new Parser();
        schemaMG.deleteRelation(parser.dropStatement(sql).trim());
    }

    //done: 1. optimize delete  2. filed size == 0  3. fill "holes"
    private static void deleteQuery(String sql){
        Statement stmt = parser.deleteStatement(sql);
        ParseTree parseTree = stmt.parseTree;
        String tableName = parseTree.tables.get(0);
        Relation relation = schemaMG.getRelation(tableName);
        int numOfBlocks = relation.getNumOfBlocks();
        //relation block index
        int i = 0;
        while(i < numOfBlocks){
            if(numOfBlocks - i <= 10){
                relation.getBlocks(i, 0, numOfBlocks - i);
                deleteQueryHelper(relation, memory, parseTree, i, numOfBlocks - i);
                break;
            }else{
                relation.getBlocks(i, 0, memory.getMemorySize());
                deleteQueryHelper(relation, memory, parseTree, i, memory.getMemorySize());
                i += 10;
            }
        }
        QueryHelper.fillHoles(relation, memory);
    }

    private static void deleteQueryHelper(Relation relation, MainMemory memory, ParseTree parseTree, int relation_block_index, int num_blocks){
        Block block;
        for(int i = 0; i < num_blocks; ++i){
            block = memory.getBlock(i);
            if(!block.isEmpty()){
                ArrayList<Tuple> tuples = block.getTuples();
                if(parseTree.where){
                    for(int j = 0; j < tuples.size(); ++j){
                        if(parseTree.expressionTree.checkTuple(tuples.get(j))){
                            block.invalidateTuple(j);
                        }
                    }
                }else{
                    block.invalidateTuples();
                }
            }
        }
        relation.setBlocks(relation_block_index, 0, num_blocks);
        QueryHelper.clearMainMem(memory);
    }

    //done: 1. handle null 2. handle "E"
    //todo: "NULL"
    private static void insertQuery(String sql){
        Statement stmt = parser.insertStatement(sql);
        Schema schema = schemaMG.getSchema(stmt.tableName);
        Relation relation = schemaMG.getRelation(stmt.tableName);
        for(int i = 0; i < stmt.fieldValues.size(); ++i){
            Tuple tuple = relation.createTuple();
            for(int j = 0; j < stmt.fieldValues.get(0).size(); ++j ){
                String value = stmt.fieldValues.get(i).get(j);
                if(schema.getFieldType(j) == FieldType.INT){
                    //handle NULL case
                    if(!value.equals("NULL")){
                        tuple.setField(stmt.fieldNames.get(j), Integer.parseInt(value));
                    }
                }else{
                    tuple.setField(stmt.fieldNames.get(j), value);
                }
            }
            appendTuple2Relation(relation, tuple, 0);
        }
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
            //be sure to clear the main memory block
            block.clear();
            block.appendTuple(tuple);
            relation.setBlock(0, memBlockIndex);
        }
    }
    public static Relation crossJoin(String tableOne, String tableTwo){
        return Join.crossProduct(schemaMG, memory, tableOne, tableTwo);
    }
    /*
    public static ArrayList<Tuple> onePassNaturalJoin(String tableOne, String tableTwo, String fieldName){
        return Join.onePassNaturalJoin(schemaMG, memory, tableOne, tableTwo, fieldName);
    }
    public static ArrayList<Tuple> twoPassNaturalJoin(String tableOne, String tableTwo, String fieldName){
        return Join.twoPassNaturalJoin(schemaMG, memory, tableOne, tableTwo, fieldName);
    }
    */

    public static void main(String[] args) throws IOException {
        Query.reset();

        Query.parseQuery("CREATE TABLE course (sid INT, homework INT, project INT, exam INT, grade STR20)");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, \"A\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 100, 100, 98, \"C\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 100, 50, 90, \"E\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 100, 100, 100, \"A\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (9, 100, 100, 66, \"A\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (6, 50, 50, 61, \"D\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (7, 0, 0, 0, \"E\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (8, 0, 0, 0, \"E\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (5, 50, 50, 59, \"D\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (10, 50, 50, 56, \"D\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (11, 0, 0, 0, \"E\")");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (12, 100, 100, 66, \"A\"");
        Query.parseQuery("INSERT INTO course (sid, homework, project, exam, grade) VALUES (20, 100, 100, 66, \"A\"");
//        Query.parseQuery("SELECT sid, grade FROM course WHERE sid > 5 ORDER BY grade");

        Query.parseQuery("CREATE TABLE course2 (sid INT, exam INT, grade STR20)");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (1, 100, \"A\")");
        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (1, 101, \"A\")");
        Query.parseQuery("SELECT * FROM course WHERE grade = \"C\" AND exam > 70 OR project > 70 AND NOT ( exam * 30 + homework * 20 + project * 50 ) / 100 < 60");
       // Query.parseQuery("SELECT * FROM course, course2");
   //     Query.parseQuery("SELECT * FROM course, course2 WHERE course.sid = course2.sid ORDER BY course.exam");
//        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (12, 25, \"E\")");
//        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
//        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 25, \"E\")");
//        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
//        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 25, \"E\")");
//        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
//        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 25, \"E\")");
//        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
//        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 25, \"E\")");
//        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (99, 100, \"A\")");
//        Query.parseQuery("INSERT INTO course2 (sid, exam, grade) VALUES (1, 25, \"E\")");
/*
        Query.parseQuery("CREATE TABLE r (a INT, b INT)");
        Query.parseQuery("CREATE TABLE s (b INT, c INT)");
        Query.parseQuery("CREATE TABLE t (a INT, c INT)");
        Query.parseQuery("INSERT INTO r (a, b) VALUES (0, 0)");
        Query.parseQuery("INSERT INTO r (a, b) VALUES (1, 1)");
        Query.parseQuery("INSERT INTO r (a, b) VALUES (1, 1)");
        Query.parseQuery("INSERT INTO r (a, b) VALUES (2, 2)");
        Query.parseQuery("INSERT INTO r (a, b) VALUES (3, 3)");
        Query.parseQuery("INSERT INTO r (a, b) VALUES (0, 0)");
        Query.parseQuery("INSERT INTO r (a, b) VALUES (1, 1)");
        Query.parseQuery("INSERT INTO r (a, b) VALUES (2, 2)");
        Query.parseQuery("INSERT INTO r (a, b) VALUES (100, 100)");
        Query.parseQuery("INSERT INTO r (a, b) VALUES (100, 100)");
        Query.parseQuery("INSERT INTO s (b, c) VALUES (0, 0)");
        Query.parseQuery("INSERT INTO s (b, c) VALUES (2, 2)");
        Query.parseQuery("INSERT INTO s (b, c) VALUES (3, 3)");
        Query.parseQuery("INSERT INTO s (b, c) VALUES (0, 0)");
        Query.parseQuery("INSERT INTO s (b, c) VALUES (0, 0)");
        Query.parseQuery("INSERT INTO s (b, c) VALUES (1, 1)");
        Query.parseQuery("INSERT INTO s (b, c) VALUES (1, 1)");
        Query.parseQuery("INSERT INTO s (b, c) VALUES (2, 2)");
        Query.parseQuery("INSERT INTO s (b, c) VALUES (101, 101)");
        Query.parseQuery("INSERT INTO s (b, c) VALUES (101, 101)");
        Query.parseQuery("INSERT INTO s (b, c) VALUES (101, 101)");
        Query.parseQuery("INSERT INTO t (a, c) VALUES (0, 0)");
        Query.parseQuery("INSERT INTO t (a, c) VALUES (1, 1)");
        Query.parseQuery("INSERT INTO t (a, c) VALUES (2, 2)");
        Query.parseQuery("INSERT INTO t (a, c) VALUES (0, 0)");
        Query.parseQuery("INSERT INTO t (a, c) VALUES (1, 1)");
        Query.parseQuery("INSERT INTO t (a, c) VALUES (1, 1)");
        Query.parseQuery("INSERT INTO t (a, c) VALUES (2, 2)");
        Query.parseQuery("INSERT INTO t (a, c) VALUES (3, 3)");
        Query.parseQuery("INSERT INTO t (a, c) VALUES (102, 102)");
        Query.parseQuery("INSERT INTO t (a, c) VALUES (102, 102)");
        Query.parseQuery("INSERT INTO t (a, c) VALUES (102, 102)");
        Query.parseQuery("INSERT INTO t (a, c) VALUES (102, 102)");

        Query.parseQuery("SELECT * FROM r, s, t WHERE r.a = t.a AND r.b = s.b AND s.c = t.c");
//        Relation relation = Query.crossJoin("course", "course2");
//        ArrayList<Tuple> tuples = Query.twoPassNaturalJoin("course", "course2", "sid");
//        for(Tuple tuple : tuples) System.out.println(tuple);
*/
    }
}
