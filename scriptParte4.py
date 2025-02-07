from neo4j import GraphDatabase

class MovieGraph:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_user(self, user_id, name):
        query = """
        MERGE (u:User {userid: $user_id})
        SET u.name = $name
        RETURN u
        """
        with self.driver.session() as session:
            session.run(query, user_id=user_id, name=name)

    def create_movie(self, movie_id, title, year, imdbRating, plot):
        query = """
        MERGE (m:Movie {movieId: $movie_id})
        SET m.title = $title, m.year = $year, m.imdbRating = $imdbRating, m.plot = $plot
        RETURN m
        """
        with self.driver.session() as session:
            session.run(query, movie_id=movie_id, title=title, year=year, imdbRating=imdbRating, plot=plot)

    def create_person(self, person_id, name, role):
        label = "Actor" if role == "ACTOR" else "Director"
        query = f"""
        MERGE (p:{label} {{personId: $person_id}})
        SET p.name = $name
        RETURN p
        """
        with self.driver.session() as session:
            session.run(query, person_id=person_id, name=name)

    def create_genre(self, name):
        query = """
        MERGE (g:Genre {name: $name})
        RETURN g
        """
        with self.driver.session() as session:
            session.run(query, name=name)

    def create_relationships(self):
        queries = [
            """
            MATCH (u:User {userid: 'u1'}), (m:Movie {movieId: 1})
            MERGE (u)-[:RATED {rating: 9, timestamp: 1707168000}]->(m)
            """,
            """
            MATCH (p:Actor {personId: 'p1'}), (m:Movie {movieId: 1})
            MERGE (p)-[:ACTED_IN {role: 'Main'}]->(m)
            """,
            """
            MATCH (p:Director {personId: 'p2'}), (m:Movie {movieId: 1})
            MERGE (p)-[:DIRECTED]->(m)
            """,
            """
            MATCH (m:Movie {movieId: 1}), (g:Genre {name: 'Sci-Fi'})
            MERGE (m)-[:IN_GENRE]->(g)
            """
        ]
        with self.driver.session() as session:
            for query in queries:
                session.run(query)

# Configurar conexión a Neo4j Aura
URI = "neo4j+s://6ce8bd8b.databases.neo4j.io"
USER = "neo4j"
PASSWORD = "LDdtWFvZIf99EfwF5Rk54Fdzkus9LDRoWPPikn9ItJI"
graph = MovieGraph(URI, USER, PASSWORD)

# Creacion de los nodos
graph.create_user("u1", "Alice")
graph.create_movie(1, "Inception", 2010, 8.8, "A mind-bending thriller")
graph.create_person("p1", "Leonardo DiCaprio", "ACTOR")
graph.create_person("p2", "Christopher Nolan", "DIRECTOR")
graph.create_genre("Sci-Fi")

# Creacion de las relaciones
graph.create_relationships()

# Siempre se cierra la conexion 
graph.close()
