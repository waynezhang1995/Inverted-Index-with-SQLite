import java.util.*;

public class VB {
	/*
    VBENCODENUMBER(n)
    1 bytes = <>
    2 while true
    3 do PREPEND(bytes, n mod 128)
    4 	if n < 128
    5 	then BREAK
    6 	n = n div 128
    7 bytes[LENGTH(bytes)] += 128
    8 return bytes
*/    
    
    public static List<Byte> VBENCODENUMBER(int n) {
    	List<Byte> bytes = new ArrayList<Byte>();
    	while (true) {
    		bytes.add(0, (byte)(n%128));
    		if (n < 128) 
    			break;
    		n = n / 128;
    	}
    	byte last  = bytes.get( bytes.size()-1 );
    	bytes.set( bytes.size()-1, (byte)(last | 0x80) ); //Achieves the effect of += 128. 
    												  //This is done like that because 
    												  //Java doesn't have unsigned byte.
    	return bytes;
    }
    
 /*
	VBENCODE(numbers)
	1 bytestream = <>
	2 for each n in numbers
	3 do bytes = VBENCODENUMBER(n)
	4 	bytestream = EXTEND(bytestream, bytes)
	5 return bytestream   
  */
    
    public static byte[] VBENCODE(List<Integer> numbers) {
    	List<Byte> bytestream_l = new ArrayList<Byte>();
    	for (Integer n : numbers) {
    		List<Byte> bytes = VBENCODENUMBER(n);
    		bytestream_l.addAll(bytes);
    	}
    	
    	//Convert result to byte[], then return. 
    	byte[] bytestream = new byte[bytestream_l.size()];
    	for (int i=0; i<bytestream_l.size(); i++) bytestream[i] = bytestream_l.get(i); 
    	return bytestream;
    }
    
/*    
    VBDECODE(bytestream)
	1 numbers = <>
	2 n = 0
	3 for i = 1 to LENGTH(bytestream)
	4 do if bytestream[i] < 128
	5 	then n = 128*n + bytestream[i]
	6 	else n = 128*n + (bytestream[i] - 128)
	7 		APPEND(numbers, n)
	8 		n = 0
	9 return numbers  
*/    
    
    public static List<Integer> VBDECODE(byte[] bytestream) {
    	List<Integer> numbers = new ArrayList<Integer>();
    	int n = 0;
    	for (int i=0; i<bytestream.length; i++) {
    		if ( (bytestream[i] & (byte)(0x80)) == 0 )  
    			n = 128*n + bytestream[i];
    		else {
    			byte b = (byte)(bytestream[i] & 0x7F); //Achieves the effect of -= 128. 
				  										   //This is done like that because 
				  										   //Java doesn't have unsigned byte.
    			n = 128*n + b;
    			numbers.add(n);
    			n = 0;
    		}
    	}
    	return numbers;
    }
    
    public static void main(String[] args) throws Exception {
    	//Testing
        List<Integer> numbers = new ArrayList<Integer>();
        numbers.add(2); numbers.add(23456); numbers.add(99999);
        numbers = VB.VBDECODE( VB.VBENCODE(numbers) );
        System.out.println(numbers);   
    }
}
