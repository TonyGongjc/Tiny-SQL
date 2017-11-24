import java.util.ArrayList;
import java.util.Arrays;

public class ExpressionTree {
    public boolean AndORNot;
    public ArrayList<Integer> AndORNots;
    public ArrayList<Integer> Ands;
    public ArrayList<Integer> ORs;
    public ArrayList<Integer> Nots;
    public ArrayList<String[]> subconditions;

    ExpressionTree(String[] condition){
        SplitSubCondition(condition);

    }


    public void SplitSubCondition(String[] condition){
        for(int i=0; i<condition.length; i++){
            String word= condition[i];
            if(word.equals("and")||word.equals("or")||word.equals("not")){
                AndORNot=true;
                if(word.equals("and")){
                    Ands.add(i);
                }else if (word.equals("or")){
                    ORs.add(i);
                }else{
                    Nots.add(i);
                }
                AndORNots.add(i);
            }else{
                AndORNot=false;
            }
        }
        if(AndORNot){
            for(int i=0; i<AndORNots.size(); i++){
                if(i==0){
                    String[] subcondition= Arrays.copyOfRange(condition, 0, AndORNots.get(i));
                    subconditions.add(subcondition);
                }else if(i==AndORNots.size()-1 && AndORNots.size()!=1){
                    String[] subcondition= Arrays.copyOfRange(condition, AndORNots.get(i)+1,condition.length);
                    subconditions.add(subcondition);
                }else{
                    String[] subcondition= Arrays.copyOfRange(condition, AndORNots.get(i)+1, AndORNots.get(i+1));
                    subconditions.add(subcondition);
                }
            }
        }
    }
}
