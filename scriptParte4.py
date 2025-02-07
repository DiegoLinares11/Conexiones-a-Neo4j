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

    def create_movie(self, movie_id, title, year, imdbRating, plot, released, runtime, countries, languages, budget, revenue, imdbVotes, poster):
        query = """
        MERGE (m:Movie {movieId: $movie_id})
        SET m.title = $title, m.year = $year, m.imdbRating = $imdbRating, m.plot = $plot,
            m.released = $released, m.runtime = $runtime, m.countries = $countries,
            m.languages = $languages, m.budget = $budget, m.revenue = $revenue,
            m.imdbVotes = $imdbVotes, m.poster = $poster
        RETURN m
        """
        with self.driver.session() as session:
            session.run(query, movie_id=movie_id, title=title, year=year, imdbRating=imdbRating, plot=plot,
                        released=released, runtime=runtime, countries=countries, languages=languages,
                        budget=budget, revenue=revenue, imdbVotes=imdbVotes, poster=poster)

    def create_person(self, person_id, name, tmdbId, born, died, bornIn, url, imdbId, bio, poster, role):
        label = "Actor" if role == "ACTOR" else "Director"
        query = f"""
        MERGE (p:{label} {{personId: $person_id}})
        SET p.name = $name, p.tmdbId = $tmdbId, p.born = $born, p.died = $died,
            p.bornIn = $bornIn, p.url = $url, p.imdbId = $imdbId, p.bio = $bio, p.poster = $poster
        RETURN p
        """
        with self.driver.session() as session:
            session.run(query, person_id=person_id, name=name, tmdbId=tmdbId, born=born, died=died,
                        bornIn=bornIn, url=url, imdbId=imdbId, bio=bio, poster=poster)

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
graph.create_movie(1, "Inception", 2010, 8.8, "A mind-bending thriller", "2010-07-16", 148, ["USA"], ["English"], 160000000, 829895144, 2000000, "inception.jpg")
graph.create_person("p1", "Leonardo DiCaprio", 6193, "1974-11-11", None, "USA", "https://www.imdb.com/name/nm0000138/", 123456, "Famous actor", "leo.jpg", "ACTOR")
graph.create_person("p2", "Christopher Nolan", 525, "1970-07-30", None, "UK", "https://www.imdb.com/name/nm0634240/", 654321, "Director and writer", "nolan.jpg", "DIRECTOR")
graph.create_genre("Sci-Fi")

# Creacion de las relaciones
graph.create_relationships()

# Siempre se cierra la conexion 
graph.close()
