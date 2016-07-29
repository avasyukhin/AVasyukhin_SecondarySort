package ssort.main;

/**
 * Created by Aphex on 29.07.2016.
 */
public class Main {
    public static void main(String[] args){
        //hardcoded test args
        args = new String[]{"data\\input.txt", "data\\output.txt"};
        try {
            Runner.execute(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
