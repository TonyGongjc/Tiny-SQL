import storageManager.*;

import java.util.ArrayList;


public class Join {
    @SuppressWarnings("Duplicates")
    public static Relation crossJoin(SchemaManager schema_manager, MainMemory memory, String tableOne, String tableTwo) {
        Relation new_relation;
        ArrayList<Tuple> tuples;

        System.out.println("CrossJoin...\n");
        Relation TableOne = schema_manager.getRelation(tableOne);
        Relation TableTwo = schema_manager.getRelation(tableTwo);
        int sizeOne = TableOne.getNumOfBlocks();
        int sizeTwo = TableTwo.getNumOfBlocks();


        Relation smallRelation;
        if (sizeOne < sizeTwo) smallRelation = TableOne;
        else smallRelation = TableTwo;

        //One pass...
        if (smallRelation.getNumOfBlocks() < memory.getMemorySize()-1) {
            tuples = onePassJoin(schema_manager, memory, tableOne, tableTwo);
        }else{
            tuples = nestedLoopJoin(schema_manager, memory, tableOne, tableTwo);
        }

        Schema schema = combineSchema(schema_manager, tableOne, tableTwo);
        String name = tableOne+"_cross_"+tableTwo;

        if(schema_manager.relationExists(name)){
            schema_manager.deleteRelation(name);
        }
        new_relation = schema_manager.createRelation(name, schema);

        int tupleNumber = tuples.size();
        int tuplesPerBlock = schema.getTuplesPerBlock();
        int tupleBlocks=0;
        if(tupleNumber<tuplesPerBlock){
            tupleBlocks = 1;
        }else if(tupleNumber>tuplesPerBlock && tupleNumber%tuplesPerBlock==0){
            tupleBlocks = tupleNumber/tuplesPerBlock;
        }else{
            tupleBlocks = tupleNumber/tuplesPerBlock +1;
        }

        int blockIndex = 0;
        while(tupleBlocks>memory.getMemorySize()){
            for(int i=0; i<memory.getMemorySize(); i++){
                Block block = memory.getBlock(i);
                block.clear();
                for(int j=0; j<tuplesPerBlock; j++){
                    if(!tuples.isEmpty()){
                        Tuple temp = tuples.get(0);
                        block.setTuple(j, temp);
                        tuples.remove(temp);
                    }else{
                        break;
                    }
                }
                tupleBlocks--;
                blockIndex++;
            }
            new_relation.setBlocks(blockIndex,0,memory.getMemorySize());
        }
        //remaining tuples
        for(int i=0; i<tupleBlocks; i++){
            Block block = memory.getBlock(i);
            block.clear();
            for(int j=0; j<tuplesPerBlock; j++){
                if(!tuples.isEmpty()){
                    Tuple temp = tuples.get(0);
                    block.setTuple(j, temp);
                    tuples.remove(temp);
                }else{
                    break;
                }
            }
            blockIndex++;
        }
        new_relation.setBlocks(blockIndex, 0, tupleBlocks);

        return new_relation;

    }

    private static ArrayList<Tuple> onePassJoin(SchemaManager schema_manager, MainMemory memory, String tableOne, String tableTwo){
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Relation TableOne = schema_manager.getRelation(tableOne);
        Relation TableTwo = schema_manager.getRelation(tableTwo);
        int sizeOne = TableOne.getNumOfBlocks();
        int sizeTwo = TableTwo.getNumOfBlocks();

        Relation smallRelation, largeRelation;
        if (sizeOne < sizeTwo) {
            smallRelation = TableOne;
            largeRelation = TableTwo;
        }
        else {
            smallRelation = TableTwo;
            largeRelation = TableOne;
        }

        Schema newSchema = combineSchema(schema_manager, tableOne, tableTwo);
        String newName="TempTable_"+tableOne+"_crossJoin_"+tableTwo;
        if(schema_manager.relationExists(newName)){
            schema_manager.deleteRelation(newName);
        }
        Relation newRelation = schema_manager.createRelation(newName, newSchema);

        //put small relation into the memory first
        smallRelation.getBlocks(0,0,smallRelation.getNumOfBlocks());

        for(int i=0; i<largeRelation.getNumOfBlocks(); i++){
            largeRelation.getBlock(i, memory.getMemorySize()-1);
            Block block = memory.getBlock(memory.getMemorySize()-1);
            ArrayList<Tuple> smallRelationTuples = memory.getTuples(0, smallRelation.getNumOfBlocks());
            ArrayList<Tuple> largeRelationTuples = block.getTuples();

            for(Tuple smallTuple:smallRelationTuples){
                for(Tuple largeTuple:largeRelationTuples){
                    if(smallRelation==TableOne){
                        tuples.add(combineTuple(newRelation, smallTuple, largeTuple));
                    }
                    else{
                        tuples.add(combineTuple(newRelation, largeTuple, smallTuple));
                    }
                }
            }
        }
        return tuples;
    }

    @SuppressWarnings("Duplicates")
    private static Schema combineSchema(SchemaManager schemaManager, String tableOne, String tableTwo){
        ArrayList<String>combineFields = new ArrayList<String>();
        ArrayList<FieldType>combineTypes = new ArrayList<FieldType>();

        Relation TableOne = schemaManager.getRelation(tableOne);
        Relation TableTwo = schemaManager.getRelation(tableTwo);

        ArrayList<String>fieldsOne = TableOne.getSchema().getFieldNames();
        ArrayList<String>fieldsTwo = TableTwo.getSchema().getFieldNames();
        ArrayList<FieldType>typesOne = TableOne.getSchema().getFieldTypes();
        ArrayList<FieldType>typesTwo = TableTwo.getSchema().getFieldTypes();

        for(String field:fieldsOne){
            if(!field.contains("\\.")){
                String newName = tableOne+"."+field;
                combineFields.add(newName);
            }
            else{
                combineFields.add(field);
            }
        }

        for(String field:fieldsTwo){
            if(!field.contains("\\.")){
                String newName = tableTwo+"."+field;
                combineFields.add(newName);
            }
            else{
                combineFields.add(field);
            }
        }

        combineTypes.addAll(typesOne);
        combineTypes.addAll(typesTwo);

        Schema newSchema = new Schema(combineFields, combineTypes);
        return newSchema;
    }


    private static Tuple combineTuple( Relation relation, Tuple smalltuple, Tuple largetuple){
        Tuple result = relation.createTuple();
        int smallSize = smalltuple.getNumOfFields();
        int largeSize = largetuple.getNumOfFields();
        for(int i = 0; i<smallSize+largeSize; i++){
            if(i<smallSize){
                String temp = smalltuple.getField(i).toString();
                if(ExpressionTree.isInteger(temp)){
                    result.setField(i, Integer.parseInt(temp));
                }else{
                    result. setField(i, temp);
                }
            }else{
                String temp = largetuple.getField(i-smallSize).toString();
                if(ExpressionTree.isInteger(temp)){
                    result.setField(i, Integer.parseInt(temp));
                }else{
                    result.setField(i, temp);
                }
            }
        }
        return result;

    }

    private static ArrayList<Tuple> nestedLoopJoin(SchemaManager schema_manager, MainMemory memory, String tableOne, String tableTwo){
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Relation TableOne = schema_manager.getRelation(tableOne);
        Relation TableTwo = schema_manager.getRelation(tableTwo);
        int sizeOne = TableOne.getNumOfBlocks();
        int sizeTwo = TableTwo.getNumOfBlocks();


        Schema newSchema = combineSchema(schema_manager, tableOne, tableTwo);

        String newName="TempTable_"+tableOne+"_crossJoin_"+tableTwo;
        if(schema_manager.relationExists(newName)){
            schema_manager.deleteRelation(newName);
        }
        Relation newRelation = schema_manager.createRelation(newName, newSchema);

        for(int i=0; i<sizeOne; i++){
            TableOne.getBlock(i,0);
            Block blockOne = memory.getBlock(0);
            for(int j=0; j<sizeTwo; j++){
                TableTwo.getBlock(j,1);
                Block blockTwo = memory.getBlock(1);
                for(Tuple tupleOne:blockOne.getTuples()){
                    for(Tuple tupleTwo:blockTwo.getTuples()){
                        tuples.add(combineTuple( newRelation, tupleOne, tupleTwo));
                    }
                }
            }
        }
        return tuples;
    }

    private static Relation naturalJoin(SchemaManager schemaManager, MainMemory memory, String tableOne, String tableTwo, String JoinOn){
        System.out.println("Natural Join...\n");
        Relation new_relation;
        ArrayList<Tuple> tuples;

        Relation TableOne = schemaManager.getRelation(tableOne);
        Relation TableTwo = schemaManager.getRelation(tableTwo);
        int sizeOne = TableOne.getNumOfBlocks();
        int sizeTwo = TableTwo.getNumOfBlocks();

        Relation smallRelation;
        if (sizeOne < sizeTwo) smallRelation = TableOne;
        else smallRelation = TableTwo;

        if (smallRelation.getNumOfBlocks() < memory.getMemorySize()-1) {
            tuples = onePassNaturalJoin(schemaManager, memory, tableOne, tableTwo, JoinOn);
        }else{
            tuples = twoPassNaturalJoin(schemaManager, memory, tableOne, tableTwo, JoinOn);
        }

    }

    private static ArrayList<Tuple> onePassNaturalJoin(SchemaManager schemaManager, MainMemory memory, String tableOne, String tableTwo, String JoinOn){
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Relation TableOne = schemaManager.getRelation(tableOne);
        Relation TableTwo = schemaManager.getRelation(tableTwo);
        int sizeOne = TableOne.getNumOfBlocks();
        int sizeTwo = TableTwo.getNumOfBlocks();

        Relation smallRelation, largeRelation;
        if (sizeOne < sizeTwo) {
            smallRelation = TableOne;
            largeRelation = TableTwo;
        }
        else {
            smallRelation = TableTwo;
            largeRelation = TableOne;
        }

        Schema newSchema = combineSchema(schemaManager, tableOne, tableTwo);
        String newName = "TempTable_"+tableOne+"_naturalJoin_"+tableTwo;
        if(schemaManager.relationExists(newName)){
            schemaManager.deleteRelation(newName);
        }
        Relation newRelation = schemaManager.createRelation(newName, newSchema);

        smallRelation.getBlocks(0,0,smallRelation.getNumOfBlocks());

        for(int i=0; i<largeRelation.getNumOfBlocks(); i++){
            largeRelation.getBlock(i, memory.getMemorySize()-1);
            Block block = memory.getBlock(memory.getMemorySize()-1);
            ArrayList<Tuple> smallRelationTuples = memory.getTuples(0, smallRelation.getNumOfBlocks());
            ArrayList<Tuple> largeRelationTuples = block.getTuples();

            for(Tuple largeTuple:largeRelationTuples){
                for(Tuple smallTuple:smallRelationTuples){
                    String result1 = smallTuple.getField(JoinOn).toString();
                    String result2 = largeTuple.getField(JoinOn).toString();
                    if(ExpressionTree.isInteger(result1) && ExpressionTree.isInteger(result2)){
                        if(Integer.parseInt(result1) == Integer.parseInt(result2)){
                            if(TableOne == smallRelation){
                                tuples.add(combineTuple(newRelation, smallTuple, largeTuple));
                            }else{
                                tuples.add(combineTuple(newRelation, largeTuple, smallTuple));
                            }
                        }else{
                            if(result1.equals(result2)){
                                if(TableOne == smallRelation){
                                    tuples.add(combineTuple(newRelation, smallTuple, largeTuple));
                                }else{
                                    tuples.add(combineTuple(newRelation, largeTuple, smallTuple));
                                }
                            }
                        }
                    }
                }
            }
        }
        return tuples;
    }

    private static ArrayList<Tuple> twoPassNaturalJoin(SchemaManager schemaManager, MainMemory memory, String tableOne, String tableTwo, String JoinOn){

    }
}
