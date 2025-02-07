from neo4j import GraphDatabase

class MovieGraph:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def find_user_movie_rating(self, user_id, movie_id):
        query = """
        MATCH (u:USER {userId: $user_id})-[r:RATED]->(m:MOVIE {movieId: $movie_id})
        RETURN u, r, m
        """
        with self.driver.session() as session:
            result = session.run(query, user_id=user_id, movie_id=movie_id)
            record = result.single()
            if record:
                user = record["u"]
                movie = record["m"]
                rating = record["r"]

                print("\n🔹 **Usuario encontrado:**")
                print(f"   🆔 ID: {user['userId']}")
                print(f"   👤 Nombre: {user['name']}")

                print("\n🎬 **Película encontrada:**")
                print(f"   🎥 Título: {movie['title']}")
                print(f"   📅 Año: {movie['year']}")
                print(f"   📝 Sinopsis: {movie['plot']}")

                print("\n⭐ **Relación RATED encontrada:**")
                print(f"   🎯 Calificación: {rating['rating']}/5")
                print(f"   ⏳ Timestamp: {rating['timestamp']}")
            else:
                print("\n⚠️ No se encontró la relación entre usuario y película.")

# Configurar conexión a Neo4j Aura
URI = "neo4j+s://6ce8bd8b.databases.neo4j.io"
USER = "neo4j"
PASSWORD = "LDdtWFvZIf99EfwF5Rk54Fdzkus9LDRoWPPikn9ItJI"

# Crear instancia de conexión
graph = MovieGraph(URI, USER, PASSWORD)

# Buscar usuario, película y relación
graph.find_user_movie_rating("u2", 1)

# Cerrar conexión
graph.close()
