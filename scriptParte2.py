from neo4j import GraphDatabase
from datetime import datetime

class MovieGraph:
    def __init__(self, uri, user, password):  # ← Aquí está corregido
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_user(self, user_id, name):
        query = """
        MERGE (u:USER {userId: $user_id})
        SET u.name = $name
        RETURN u
        """
        with self.driver.session() as session:
            session.run(query, user_id=user_id, name=name)

    def create_movie(self, movie_id, title, year, plot):
        query = """
        MERGE (m:MOVIE {movieId: $movie_id})
        SET m.title = $title, m.year = $year, m.plot = $plot
        RETURN m
        """
        with self.driver.session() as session:
            session.run(query, movie_id=movie_id, title=title, year=year, plot=plot)

    def create_rating(self, user_id, movie_id, rating, timestamp):
        query = """
        MATCH (u:USER {userId: $user_id}), (m:MOVIE {movieId: $movie_id})
        MERGE (u)-[r:RATED]->(m)
        SET r.rating = $rating, r.timestamp = $timestamp
        RETURN r
        """
        with self.driver.session() as session:
            session.run(query, user_id=user_id, movie_id=movie_id, rating=rating, timestamp=timestamp)


#Tiempo de ahora.
current_timestamp = int(datetime.now().timestamp())
# Configurar conexión con Neo4j Aura
URI = "neo4j+s://6ce8bd8b.databases.neo4j.io"
USER = "neo4j"
PASSWORD = "LDdtWFvZIf99EfwF5Rk54Fdzkus9LDRoWPPikn9ItJI"  

# Crear instancia del grafo
graph = MovieGraph(URI, USER, PASSWORD)

# Crear nodos y relaciones
graph.create_user("u1", "Alice")
graph.create_user("u2", "Bob")
graph.create_user("u3", "Davis")
graph.create_user("u4", "Andy")
graph.create_user("u5", "Jorge")

graph.create_movie(2, "Seven", 1995, "Detectives track a serial killer")

graph.create_rating("u2", 2, 5, current_timestamp)
graph.create_rating("u2", 1, 3, current_timestamp)
graph.create_rating("u3", 1, 3, current_timestamp)
graph.create_rating("u3", 2, 4, current_timestamp)
graph.create_rating("u4", 1, 3, current_timestamp)
graph.create_rating("u4", 2, 4, current_timestamp)
graph.create_rating("u5", 1, 3, current_timestamp)
graph.create_rating("u5", 2, 5, current_timestamp) 

# Cerrar conexión
graph.close()
