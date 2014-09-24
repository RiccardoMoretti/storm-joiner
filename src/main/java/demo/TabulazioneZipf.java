package demo;

import java.io.FileWriter;

import joiner.computational.ZipfGenerator;

public class TabulazioneZipf {
	
	private final static int DOMAINSTARTSAT  = 0;
	private final static int DOMAINENDSAT  = 100;
	
	private final static int TUPLETABLE  = 100;

	public static void main(String[] args) throws Exception {
		
	int	rand[] = new int[TUPLETABLE];
	int cont[] = new int[DOMAINENDSAT - DOMAINSTARTSAT];
			
	ZipfGenerator Z = new ZipfGenerator ((int) DOMAINENDSAT - DOMAINSTARTSAT , 1 );
	
	for (int i = 0 ; i < TUPLETABLE ; ++i)	
	{	
		rand[i] = Z.nextInt() + DOMAINSTARTSAT;
	}
		
	for (int i = 0 ; i < TUPLETABLE ; ++i)	
	{	
		cont[rand[i]]++;
	}
	
	for ( int i = 0 ; i < DOMAINENDSAT - DOMAINSTARTSAT ; i++ )			
	{
	     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/ZIpf.txt";
	     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
		 fw.write(System.lineSeparator()+cont[i]);//appends the string to the file
		 fw.close();
	}	
}
}
