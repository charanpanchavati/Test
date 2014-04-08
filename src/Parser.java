
public class Parser {

	public static void main(String [] args){
		
		String str = new String("12::Movie Name :: Romance| XXX | Blue");
		String [] threeParts = new String[3];
		threeParts = str.split("::");
		//System.out.println(threeParts[0]  +", " +threeParts[1]+ ", " +threeParts[2]);
		String [] genres = threeParts[2].split("\\|");
		System.out.println("Genres: ");
		for(int i=0; i<genres.length; i++){
			System.out.println(genres[i].trim());
		}
	}
			
}
