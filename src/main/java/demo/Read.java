	import java.io.InputStreamReader;
	import java.io.BufferedReader;
	import java.io.IOException;
	 
	public class Read
	{
	   public String readString()
	   {
	      br = new BufferedReader(new InputStreamReader(System.in));
	 
	      try
	      {
	         _String = br.readLine();
	      }
	      catch (IOException e)
	      {
	         System.out.println ("errore di flusso");
	      }
	 
	      return(_String);
	   }
	 
	   public int readInt()
	   {
	      br = new BufferedReader(new InputStreamReader(System.in));
	 
	      try
	      {
	         _String = br.readLine();
	         _int = Integer.parseInt(_String);
	      }
	      catch (IOException e1)
	      {
	         System.out.println ("errore di flusso");
	      }
	      catch (NumberFormatException e2)
	      {
	         System.out.println ("errore di input da tastiera");
	         return(0);
	      }
	 
	      return(_int);
	   }
	 
	   public char readChar()
	   {
	      br = new BufferedReader(new InputStreamReader(System.in));
	 
	      try
	      {
	         _String = br.readLine();
	 
	         if (_String.length() > 1)
	            throw new NumberFormatException();
	 
	         _char = _String.charAt(0);
	      }
	      catch (IOException e1)
	      {
	         System.out.println ("errore di flusso");
	      }
	      catch (NumberFormatException e2)
	      {
	         System.out.println ("errore di input da tastiera");
	         return(0);
	      }
	 
	      return(_char);
	   }
	 
	   public float readFloat()
	   {
	      br = new BufferedReader(new InputStreamReader(System.in));
	 
	      try
	      {
	         _String = br.readLine();
	         _float = Float.parseFloat(_String);
	      }
	      catch (IOException e1)
	      {
	         System.out.println ("errore di flusso");
	      }
	      catch (NumberFormatException e2)
	      {
	         System.out.println ("errore di input da tastiera");
	         return(0);
	      }
	 
	      return(_float);
	   }
	 
	   public double readDouble()
	   {
	      br = new BufferedReader(new InputStreamReader(System.in));
	 
	      try
	      {
	         _String = br.readLine();
	         _double = Double.parseDouble(_String);
	      }
	      catch (IOException e1)
	      {
	         System.out.println ("errore di flusso");
	      }
	      catch (NumberFormatException e2)
	      {
	         System.out.println ("errore di input da tastiera");
	         return(0);
	      }
	 
	      return(_double);
	   }
	 
	   private  BufferedReader br;
	 
	   private  String _String;
	   private  int _int;
	   private  char _char;
	   private  float _float;
	   private  double _double;
	

	
	  /* public void main(String[] arg)
	   {
	      System.out.print("inserisci un float: ");
	      float e = Read.readFloat();
	      System.out.println(e);
	 
	      System.out.print("inserisci un int: ");
	      int a = Read.readInt();
	      System.out.println(a);
	   }
*/	
}