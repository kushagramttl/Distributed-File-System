package Helper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Utils {
    public static boolean areDistinct(String arr[]) {
        // Put all array elements in a HashSet
        Set<String> s = new HashSet<String>(Arrays.asList(arr));
        // If all elements are distinct, size of
        // HashSet should be same array.
        return (s.size() == arr.length);
    }
}
