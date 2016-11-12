package database;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import static java.util.Arrays.asList;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;

public class EjemploMongoDB {
	
	public static void main(String args[]) throws ParseException {
		//Conectar a instancia MongoDB
		@SuppressWarnings("resource")
		MongoClient mongoClient = new MongoClient("localhost",27017);
		//Acceder a la base de datos test
		MongoDatabase db = mongoClient.getDatabase("test");
		
		//*********** INSERTAR ***********//
		//Insertar un elemento en la coleccion
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
		db.getCollection("restaurants").insertOne(
				new Document("address", 
						new Document()
							.append("street", "2 Avenue")
							.append("zipcode", "10075")
							.append("building", "1480")
							.append("coord", asList(-73.9557413, 40.7720266)))
				.append("borough", "Manhattan")
				.append("cuisine", "Italian")
				.append("grades", asList(
						new Document()
							.append("date", format.parse("2014-10-01T00:00:00Z"))
							.append("grade", "A")
							.append("score", 11),
						new Document()
							.append("date", format.parse("2014-10-16T00:00:00Z"))
							.append("grade", "B")
							.append("score", 17)
						)
					)
				.append("name", "Vella")
				.append("reastaurant_id", "41704620")
			);
		
		
		//*********** CONSULTAR ***********//
		//Consultar todos los documentos de la coleccion
		/*FindIterable<Document> iterable = db.getCollection("restaurants").find();*/
		
		//Consultar por un campo de primer nivel
		/*FindIterable<Document> iterable = db.getCollection("restaurants").find(
				new Document("bourough", "Manhattan"));*/	
		
		//Consultar por un campo en un documento embebido
		/*FindIterable<Document> iterable = db.getCollection("restaurants").find(
				new Document("address.street", "Morris Park Ave"));*/
		
		//Consultar por un campo en un documento embebido (otra forma)
		/*FindIterable<Document> iterable = db.getCollection("restaurants").find(
				eq("address.street", "Morris Park Ave"));*/
		
		//Consultar por un campo en un array
		/*FindIterable<Document> iterable = db.getCollection("restaurants").find(
				new Document("grades.grade", "B"));*/
		
		//Consultar comparando con el operador mayor que
		/*FindIterable<Document> iterable = db.getCollection("restaurants").find(
				new Document("grades.score", new Document("$gt",30)));*/
		
		//Consultar comparando con el operador menor que
		/*FindIterable<Document> iterable = db.getCollection("restaurants").find(
				new Document("grades.score", new Document("$lt",10)));*/
		
		//Combinar condiciones con AND
		/*FindIterable<Document> iterable = db.getCollection("restaurants").find(
				new Document("cuisine","Italian")
				.append("address.zipcode", "10075"));*/
		
		//Combinar condiciones con AND (otra forma)
		/*FindIterable<Document> iterable = db.getCollection("restaurants").find(
				and(eq("cuisine","Italian"), eq("address.zipcode", "10075")));*/
		
		//Combinar condiciones con OR
		/*FindIterable<Document> iterable = db.getCollection("restaurants").find(
				new Document("$or", asList(new Document("cuisine","Italian"),
						new Document("address.zipcode","10075"))));*/
		
		//Combinar condiciones con OR (otra forma)
		FindIterable<Document> iterable = db.getCollection("restaurants").find(
				or(eq("cuisine","Italian"), eq("address.zipcode", "10075")));
		
		
		//Ordenar resultados
		/*FindIterable<Document> iterable = db.getCollection("restaurants").find()
				.sort(new Document("borough",1).append("address.zipcode", 1));*/
		
		//Ordenar resultados (otra forma)
		/*FindIterable<Document> iterable = db.getCollection("restaurants").find()
				.sort(ascending("borough","address.zipcode"));*/
		
		iterable.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				System.out.println(document);
			}
		});
		

		//*********** ACTUALIZAR ***********//
		//Actualizar un campo de primer nivel
		//Actualiza el primer documento cuyo campo name es igual a Juni
		//Cambia cuisine y lastModified 
		db.getCollection("restaurants").updateOne(new Document("name", "Juni"), 
				new Document("$set", new Document("cuisine", "American (New)"))
				.append("$currentDate", new Document("lastModified", true)));
		
		//Actualizar un campo en un documento embebido
		db.getCollection("restaurants").updateOne(new Document("restaurant_id", "41156888"), 
				new Document("$set", new Document("address.street", "East 31st Street")));
		
		//Actualizar multiples documentos
		db.getCollection("restaurants").updateMany(new Document("address.zipcode", "10016")
				.append("cuisine", "Other"), 
				new Document("$set", new Document("cuisine", "Category To Be Determined"))
				.append("$currentDate", new Document("lastModified", true)));
		
		//Reemplazar un documento entero
		db.getCollection("restaurants").replaceOne(new Document("restaurant_id", "41704620"), 
				new Document("address", new Document()
					.append("street", "2 Avenue")
					.append("zipcode", "10075")
					.append("building", "1480")
					.append("coord", asList(-73.9557413, 40.7720266)))
				.append("name", "Vella 2"));
		
		
		
		//*********** ELIMINAR ***********//
		//Eliminar todos los documentos que cumplen una condicion
		db.getCollection("restaurants").deleteMany(new Document("borough", "Manhattan"));
		
		//Eliminar todos los documentos
		db.getCollection("restaurants").deleteMany(new Document());
		
		//Dropear una coleccion
		db.getCollection("restaurants").drop();
		
	}
}
