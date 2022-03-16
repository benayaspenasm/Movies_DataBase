const contents = [
    {
        content: "C:\\Users\\usuario\\Desktop\\Master UCM\\2 Bases de datos NoSQL - Alvaro Bravo\\TAREA\\movies.json",
        collection: "movies",
        idPolicy: "drop_collection_first", //overwrite_with_same_id|always_insert_with_new_id|insert_with_new_id_if_id_exists|skip_documents_with_existing_id|abort_if_id_already_exists|drop_collection_first|log_errors
        //Use the transformer to customize the import result
        //transformer: (doc)=>{ //async (doc)=>{
        //   doc["importDate"]= new Date()
        //   return doc; //return null skips this doc
        //}
    }
];

mb.importContent({
    connection: "localhost",
    database: "clases",
    fromType: "file",
    batchSize: 2000,
    contents
})
// ver las bases de datos disponibles
show dbs

use clases
// ver la base de datos en la que se trabaja -> clases
db

// listar colecciones de la base de datos "clases" ->"clases.movies"
show collections

// 1) Analizar con find la colección
db.movies.find()

// 2) Contar cuántos documentos (películas tiene cargado)
db.movies.find().count()

// 3) Insertar una película
var nuevo_item = { "title": "paraborrar", "year": 2050, "cast": ["c1", "c2"], "genres": ["g1", "g2"] }
db.movies.insertOne(nuevo_item)
db.movies.find().count() // vemos que la cuenta está ahora en 28795+1=28796

// 4) Borrar la película insertada en el punto anterior
db.movies.deleteMany({ "title": "paraborrar" }) // mejor que deleteOne() por si hemos dado (sin querer) varias veces al insertOne
db.movies.find().count() // vuelve a ser 28795

// 5) Contar cuantas películas tienen actores (cast) que se llaman “and".
var query = { "cast": { $in: ["and"] } }
//db.movies.find(query)
db.movies.find(query).count()

// 6) Actualizar los documentos cuyo actor (cast) tenga por error el valor “and” como si realmente fuera un actor.
// Para ello, se debe sacar únicamente ese valor del array cast.
var query = { "cast": { $in: ["and"] } }
var operacion = { $pull: { "cast": "and" } }
db.movies.updateMany(query, operacion)

// 7) Contar cuantos documentos (películas) no tienen actores (array cast).
var query = { "cast": { $size: 0 } }
db.movies.find(query).count()

// 8) Actualizar TODOS los documentos (películas) que no tengan actores (array cast),
//    añadiendo un nuevo elemento dentro del array con valor Undefined.
var query = { "cast": { $size: 0 } }
var operacion = { $push: { "cast": "Undefined" } }
db.movies.updateMany(query, operacion)
var query = { "cast": "Undefined" }
db.movies.find(query)

// 9) Contar cuantos documentos (películas) no tienen Género (array genres)
var query = { "genres": { $size: 0 } }
db.movies.find(query).count()

// 10) Actualizar TODOS los documentos (películas) que no tengan géneros (array genres), 
// añadiendo un nuevo elemento dentro del array con valor Undefined.
var query = { "genres": { $size: 0 } }
var operacion = { $push: { "genres": "Undefined" } }
db.movies.updateMany(query, operacion)


// Casos donde tanto cast como genres son Undefined
var query = { "genres": "Undefined" }
var query2 = { "cast": "Undefined" }
var filter = { $and: [query, query2] }
db.movies.find(filter)

// 11) Mostrar el año más reciente / actual que tenemos sobre todas las películas.
db.movies.aggregate(
    [
        { $sort: { "year": -1 } }, { $limit: 1 }, { $project: { "_id": 0, "year": 1 } }
    ]
)// no modifica coleccion

// 12) Contar cuántas películas han salido en los últimos 20 años. 
// Debe hacerse desde el último año que se tienen registradas películas en la colección, mostrando el resultado total de esos años.
var fase1 = { $group: { "_id": "$year", "total": { $sum: 1 } } }
var fase2 = { $project: { "_id": 0, "year": "$_id" "total": 1 } }
var fase3 = { $sort: { "year": -1 } }
var fase4 = { $limit: 20 }
var fase5 = { $group: { "_id": null, "total": { $sum: "$total" } } }
db.movies.aggregate([fase1, fase2, fase3, fase4, fase5])

// 13) Contar cuántas películas han salido en la década de los 60 (del 60 al 69 incluidos).
var fase1 = { $group: { "_id": "$year", "total": { $sum: 1 } } }
var fase2 = { $match: { "_id": { $gte: 1960, $lte: 1969 } } }
var fase3 = { $group: { "_id": null, "total": { $sum: "$total" } } }
db.movies.aggregate([fase1, fase2, fase3])


// 14) Mostrar el año u años con más películas mostrando el número de películas de ese año. 
// Revisar si varios años pueden compartir tener el mayor número de películas.
var docu = { "year": "$year" }
var fase1 = { $group: { "_id": "$year", "numero_peliculas": { $sum: 1 } } }
var fase2 = { $project: { "_id": 0, "year": "$_id", "numero_peliculas": 1 } }
var fase3 = { $group: { _id: "$numero_peliculas", "number_years": { $sum: 1 }, "items": { $push: docu } } }
var fase4 = { $project: { "_id": 0, "numero_peliculas": "$_id", "years": "$items" } }
var fase5 = { $sort: { "numero_peliculas": -1 } }
var fase6 = { $limit: 1 }
var fase7={$project:{"pelis":"$numero_peliculas","_id":"$years.year"}}
db.movies.aggregate([fase1, fase2, fase3, fase4, fase5, fase6,fase7])

// 15) Mostrar el año u años con menos películas mostrando el número de películas de ese año. 
// Revisar si varios años pueden compartir tener el menor número de películas.
var docu = { "year": "$year" }
var docu = { "year": "$year" }
var fase1 = { $group: { "_id": "$year", "numero_peliculas": { $sum: 1 } } }
var fase2 = { $project: { "_id": 0, "year": "$_id", "numero_peliculas": 1 } }
var fase3 = { $group: { _id: "$numero_peliculas", "number_years": { $sum: 1 }, "items": { $push: docu } } }
var fase4 = { $project: { "_id": 0, "numero_peliculas": "$_id", "years": "$items" } }
var fase5 = { $sort: { "numero_peliculas": 1 } }
var fase6 = { $limit: 1 }
db.movies.aggregate([fase1, fase2, fase3, fase4, fase5, fase6])

// 16) Guardar en nueva colección llamada “actors” realizando la fase $unwind por actor. 
// Después, contar cuantos documentos existen en la nueva colección.
//db.movies.find()

var fase1 = { $set: { "size_of_cast": { $size: "$cast" } } }
var fase2 = { $match: { "size_of_cast": { $gte: 1 } } }
var fase3 = { $project: {"size_of_cast": 0 } }
var fase4 = { $unwind: "$cast" }
var fase5 = { $project: {"_id": 0 } } // Para evitar claves duplicadas en la db
var fase6 = { $out: "actors" }

db.movies.aggregate([fase1, fase2, fase3, fase4,fase5,fase6])

db.actors.find().count()

show collections

// 17) Sobre actors (nueva colección), mostrar la lista con los 5 actores que han participado en más películas mostrando
// el número de películas en las que ha participado.

//db.actors.find()

var fase1 = { $match: { "cast": { $ne: 'Undefined' }}}
var fase2 = { $group: { "_id": "$cast", "numero_peliculas": { $sum: 1 } } }
var fase3 = { $sort: { "numero_peliculas": -1 } }
var fase4 = { $limit: 5 }
db.actors.aggregate([fase1,fase2,fase3,fase4])

// 18) Sobre actors (nueva colección), agrupar por película y año mostrando las 5 en las que más actores hayan participado,
// mostrando el número total de actores.

var fase2= { $group: { "_id": { "title": "$title" , "year":"$year" } , "cuenta":{$sum:1 }}}
var fase3 = { $sort: { "cuenta": -1 } }
var fase4 = { $limit: 5 }
db.actors.aggregate([fase2,fase3,fase4])


// 19) Sobre actors (nueva colección), mostrar los 5 actores cuya carrera haya sido la más larga. 
// Para ello, se debe mostrar cuándo comenzó su carrera, cuándo finalizó y cuántos años han pasado
    
var fase1={ $match: { "cast": { $ne: 'Undefined' }}}
var docu={"year": "$year" }
var fase2 = { $group: { _id: "$cast", "years": { $push: docu } } }
var fase3= { $project: { "_id": 1, "years": "$years.year" } }
var fase4={ $addFields: { "comienza": {$min: "$years"}, "termina":{$max: "$years"} } }
var fase5={ $addFields: { "anos": {$subtract: ["$termina","$comienza"]} } }
var fase6={ $project: { "_id": 1, "comienza": 1 , "termina": 1 ,"anos":1} }
var fase7 = { $sort: { "anos": -1 } }
var fase8 = { $limit: 5 }
db.actors.aggregate([fase1,fase2,fase3,fase4,fase5,fase6,fase7,fase8])

// 20) Sobre actors (nueva colección), Guardar en nueva colección llamada “genres” realizando la fase $unwind por genres.
// Después, contar cuantos documentos existen en la nueva colección

var fase1 = { $unwind: "$genres" }
var fase2 = { $project: {"_id": 0 } } // Para evitar claves duplicadas en la db
var fase3 = { $out: "genres" }
db.movies.aggregate([fase1,fase2,fase3])

db.genres.find().count()

// 21) Sobre genres (nueva colección), mostrar los 5 documentos agrupados por “Año y Género” que más número de películas diferentes 
// tienen mostrando el número total de películas.

db.genres.find()

var docu = { "title": "$title" }
var fase1= { $group: { "_id": { "year": "$year" , "genre":"$genres" } , "pelis": { $push: docu } }}
var fase2={ $unwind: "$pelis" }
var fase3= { $project: { "_id": 0,"ano_genero":"$_id", "pelis": "$pelis.title" } }
var fase4={$group:{"_id": { "ano_genero": "$ano_genero" , "pelis":"$pelis" }, "count":{$sum:1}}}
//var fase5={$match:{count:{$lt:2}}}
// Ajustamos al formato de la diapositiva en la fase6, tras haber filtrado los duplicados en fase4
var fase5={$project:{"_id": { "year": "$_id.ano_genero.year" , "genre":"$_id.ano_genero.genre" }, "pelis":"$_id.pelis"}}
var fase6={$group:{"_id":"$_id", "count":{$sum:1}}}
var fase7 = { $sort: { "count": -1 } }
var fase8 = { $limit: 5 }
db.genres.aggregate([fase1,fase2,fase3,fase4,fase5,fase6,fase7,fase8])

// 22) Sobre genres (nueva colección), mostrar los 5 actores y los géneros en los que han participado con más número de géneros diferentes, 
// se debe mostrar el número de géneros diferentes que ha interpretado.

var fase1={ $match: { "cast": { $ne: 'Undefined' }}}
var fase2={ $unwind: "$cast" }
var fase3={ $group: { "_id": { "cast": "$cast" , "genres":"$genres" },"count":{$sum:1} }}
var fase4={$project:{"_id":"$_id.cast","genress":"$_id.genres"}}
var docu = { "generos": "$genress" }
var fase5={$group:{"_id":"$_id","numgeneros":{$sum:1},"generos": { $push: docu }}}
var fase6={$project:{"_id":1,"numgeneros":1, "generos":"$generos.generos"} }
var fase7 = { $sort: { "numgeneros": -1 } }
var fase8 = { $limit: 5 }
db.genres.aggregate([fase1,fase2,fase3,fase4,fase5,fase6,fase7,fase8])

// 23) Sobre genres (nueva colección), mostrar las 5 películas y su año correspondiente en los que más géneros diferentes han sido catalogados,
// mostrando esos géneros y el número de géneros que contiene.


//var fase1={ $unwind: "$cast" }
var docu = { "generos": "$genres" }
var fase2={ $group: { "_id": { "title": "$title", "year":"$year"},"generos": { $push: docu } }}
var fase3={$project:{"_id":1, "generos":"$generos.generos"} }
var fase4={ $unwind: "$generos" }
var fase5={ $group: { "_id": { "_id": "$_id" , "generos":"$generos" },"count":{$sum:1} }}
var fase6={$project :{"_id":"$_id._id", "generos":"$_id.generos"}}
var docu = { "generos": "$generos" }
var fase7={$group:{"_id":"$_id","numgeneros":{$sum:1},"generos": { $push: docu }}}

var fase8 = { $sort: { "numgeneros": -1 } }
var fase9 = { $limit: 5 }
var fase10={ $project: {generos:"$generos.generos"} }
db.genres.aggregate([fase2,fase3,fase4,fase5,fase6,fase7,fase8,fase9,fase10])

db.genres.find()

// 24) Consulta libre sobre el pipeline de agregación

// Crear la colección "guerras". Contar cuantas películas salieron en el último año de alguna guerra de la colección "guerras".

db.guerras.drop()// evitar insertar muchas veces y generar duplicados
db.guerras.insert([
   { "_id" : 1, "nombre" : "Guerra Mundial I", "inicio" : 1914, "fin" : 1918 },
   { "_id" : 2, "nombre" : "Guerra Mundial II", "inicio" : 1939, "fin" : 1945 },
   { "_id" : 3, "nombre" : "Guerra Civil Espanola", "inicio" : 1936, "fin" : 1939 }
])

// db.guerras.find()

fase1=   {  $lookup: { from: "movies", localField: "fin", foreignField: "year",  as: "movies_docs" }  }
fase2={ $unwind: "$movies_docs" }
var fase3={ $group: { "_id": null,"count":{$sum:1} }}
db.guerras.aggregate([fase1,fase2,fase3])

// 25) Consulta libre sobre el pipeline de agregación

// Remplazar el documento de la Segunda Guerra Mundial de la colección "guerras" añadiendo un nuevo campo con la duración de esa guerra y 
// guardar ese resultado en una nueva colección
db.guerras1.drop()
var fase1={ $match: { "nombre": "Guerra Mundial II" } }
var fase2=   { $replaceWith: { _id: "$_id", nombre: "$nombre", inicio:"$inico", fin:"$fin", duracion: { $subtract: [ "$fin", "$inicio"]} } }
var fase3 = { $out: "guerras1" }
db.guerras.aggregate([fase1,fase2,fase3])
db.guerras1.find()

// 26) Consulta libre sobre el pipeline de agregación

// Calcular cuántos años han pasado desde el fin de las guerras de la colección “guerras”
var fase1=   { $set: { current_date: "$$NOW" } }
var fase2=   { $replaceWith:{ _id: "$_id", nombre: "$nombre", inicio:"$inico", fin:"$fin",current_date:{ $year: "$current_date" }}}
var fase3={ $addFields: {tiempo_desde_fin:{ $subtract: [ "$current_date","$fin"]}} }
var fase4={ $project: {"_id":0,"nombre": 1 ,"tiempo_desde_fin":1 } }
db.guerras.aggregate([fase1,fase2,fase3,fase4])

