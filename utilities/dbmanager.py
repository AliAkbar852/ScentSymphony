import pyodbc
import logging
from config import DB_CONNECTION_STRING

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DBManager:
    """Manages all database operations for the perfume scraper with MS SQL Server."""

    def __init__(self, connection_string=DB_CONNECTION_STRING):
        self.connection_string = connection_string
        self.conn = None
        self.cursor = None

    def _connect(self):
        """Establishes a connection to the SQL Server database."""
        try:
            self.conn = pyodbc.connect(self.connection_string, autocommit=False)
            self.cursor = self.conn.cursor()
        except pyodbc.Error as e:
            logging.error(f"❌ Database connection failed: {e}")
            raise

    def _close(self):
        """Closes the database cursor and connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def create_tables(self):
        """Creates all necessary tables if they don't exist."""
        self._connect()
        try:
            logging.info("Checking and creating all database tables if they don't exist...")

            # 1. Countries
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.Countries', N'U') IS NULL
            CREATE TABLE Countries (
                country_id INT IDENTITY(1,1) PRIMARY KEY, 
                country_name NVARCHAR(255) UNIQUE NOT NULL,
                brand_count INT
            )''')

            # 2. Brands
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.Brands', N'U') IS NULL
            CREATE TABLE Brands (
                id INT IDENTITY(1,1) PRIMARY KEY,
                brand_name NVARCHAR(255) UNIQUE NOT NULL, 
                country_id INT FOREIGN KEY REFERENCES Countries(country_id), 
                brand_url NVARCHAR(500),
                perfume_count INT,
                brand_website_url NVARCHAR(500), 
                brand_image_url NVARCHAR(500) 
            )''')

            # 3. Perfumes
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.Perfumes', N'U') IS NULL
            CREATE TABLE Perfumes (
                perfume_id INT IDENTITY(1,1) PRIMARY KEY,
                perfume_name NVARCHAR(255), 
                perfume_for NVARCHAR(255), 
                image_url NVARCHAR(500),
                launch_year INT,
                perfumer_name NVARCHAR(255),
                perfumer_url NVARCHAR(500),
                perfume_url NVARCHAR(500) UNIQUE NOT NULL, 
                brand_id INT NULL FOREIGN KEY REFERENCES Brands(id)
            )''')

            # 4. Notes (No changes here)
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.Notes', N'U') IS NULL
            CREATE TABLE Notes (
                note_id INT IDENTITY(1,1) PRIMARY KEY,
                note_name NVARCHAR(255) UNIQUE NOT NULL
            )''')

            # 5. Accords (No changes here)
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.Accords', N'U') IS NULL
            CREATE TABLE Accords (
                accord_id INT IDENTITY(1,1) PRIMARY KEY,
                accord_name NVARCHAR(255) UNIQUE NOT NULL
            )''')

            # 6. PerfumeNotes
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.PerfumeNotes', N'U') IS NULL
            CREATE TABLE PerfumeNotes (
                perfume_id INT FOREIGN KEY REFERENCES Perfumes(perfume_id) ON DELETE CASCADE,
                note_id INT FOREIGN KEY REFERENCES Notes(note_id) ON DELETE CASCADE,
                note_level NVARCHAR(50), 
                PRIMARY KEY (perfume_id, note_id, note_level) 
            )''')

            # 7. PerfumeAccords (No changes here)
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.PerfumeAccords', N'U') IS NULL
            CREATE TABLE PerfumeAccords (
                perfume_id INT FOREIGN KEY REFERENCES Perfumes(perfume_id) ON DELETE CASCADE,
                accord_id INT FOREIGN KEY REFERENCES Accords(accord_id) ON DELETE CASCADE,
                accord_strength DECIMAL(5,2) NULL,
                PRIMARY KEY (perfume_id, accord_id)
            )''')

            # 8. PerfumeVotes (No changes here)
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.PerfumeVotes', N'U') IS NULL
            CREATE TABLE PerfumeVotes (
                vote_id INT IDENTITY(1,1) PRIMARY KEY,
                perfume_id INT NOT NULL,
                review_count INT NOT NULL DEFAULT 0,
                rating_count INT NOT NULL DEFAULT 0,
                rating_value DECIMAL(3, 2) NOT NULL,
                FOREIGN KEY (perfume_id) REFERENCES Perfumes(perfume_id) ON DELETE CASCADE
            )''')

            # 9. PerfumePercentages (No changes here)
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.PerfumePercentages', N'U') IS NULL
            CREATE TABLE PerfumePercentages (
                percentage_id INT IDENTITY(1,1) PRIMARY KEY,
                perfume_id INT NOT NULL,
                category VARCHAR(50) NOT NULL,
                label VARCHAR(50) NOT NULL,
                percentage_value DECIMAL(5, 2) NOT NULL,
                FOREIGN KEY (perfume_id) REFERENCES Perfumes(perfume_id) ON DELETE CASCADE
            )''')

            # 10. PerfumeStats (No changes here)
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.PerfumeStats', N'U') IS NULL
            CREATE TABLE PerfumeStats (
                stat_id INT IDENTITY(1,1) PRIMARY KEY,
                perfume_id INT NOT NULL,
                category VARCHAR(50) NOT NULL,
                label VARCHAR(50) NOT NULL,
                vote_count INT NOT NULL DEFAULT 0,
                FOREIGN KEY (perfume_id) REFERENCES Perfumes(perfume_id) ON DELETE CASCADE
            )''')

            # 11. Reviews
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.Reviews', N'U') IS NULL
            CREATE TABLE Reviews (
                review_id INT IDENTITY(1,1) PRIMARY KEY,
                perfume_id INT NOT NULL,
                review_content NVARCHAR(MAX), 
                reviewer_name NVARCHAR(250), 
                review_date DATE, 
                FOREIGN KEY (perfume_id) REFERENCES Perfumes(perfume_id) ON DELETE CASCADE
            )''')

            self.conn.commit()
            logging.info("All tables checked/created successfully.")

        except pyodbc.Error as e:
            logging.error(f"Error creating tables: {e}")
            self.conn.rollback()
        finally:
            self._close()

    def clear_perfume_details(self, perfume_id):
        self._connect()
        try:
            self.cursor.execute("DELETE FROM PerfumeVotes WHERE perfume_id = ?", perfume_id)
            self.cursor.execute("DELETE FROM PerfumePercentages WHERE perfume_id = ?", perfume_id)
            self.cursor.execute("DELETE FROM PerfumeStats WHERE perfume_id = ?", perfume_id)
            self.cursor.execute("DELETE FROM Reviews WHERE perfume_id = ?", perfume_id)
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error clearing details for PerfumeID {perfume_id}: {e}")
            self.conn.rollback()
        finally:
            self._close()

    def insert_perfume_vote(self, perfume_id, data):
        self._connect()
        try:
            self.cursor.execute("""
                                INSERT INTO PerfumeVotes (perfume_id, review_count, rating_count, rating_value)
                                VALUES (?, ?, ?, ?)
                                """, (
                                    perfume_id,
                                    int(data.get("review_count", 0)),
                                    int(data.get("rating_count", 0)),
                                    float(data.get("rating_value", 0.0))
                                ))
            self.conn.commit()
        except Exception as e:
            logging.error(f"Failed to insert vote data for PerfumeID {perfume_id}: {e}")
            self.conn.rollback()
        finally:
            self._close()

    def insert_perfume_percentages(self, perfume_id, category, data_dict):
        self._connect()
        try:
            for label, percent_str in data_dict.items():
                try:
                    percentage = float(str(percent_str).strip('%'))
                    self.cursor.execute("""
                                        INSERT INTO PerfumePercentages (perfume_id, category, label, percentage_value)
                                        VALUES (?, ?, ?, ?)
                                        """, (perfume_id, category, label, percentage))
                except (ValueError, TypeError):
                    logging.warning(f"Could not parse percentage '{percent_str}' for {category} - {label}. Skipping.")
            self.conn.commit()
        except Exception as e:
            logging.error(f"Failed to insert percentage data for PerfumeID {perfume_id}, Category {category}: {e}")
            self.conn.rollback()
        finally:
            self._close()

    def insert_perfume_stats(self, perfume_id, category, data_dict):
        self._connect()
        try:
            for label, votes in data_dict.items():
                try:
                    self.cursor.execute("""
                                        INSERT INTO PerfumeStats (perfume_id, category, label, vote_count)
                                        VALUES (?, ?, ?, ?)
                                        """, (perfume_id, category, label, int(votes)))
                except (ValueError, TypeError):
                    logging.warning(f"Could not parse vote count '{votes}' for {category} - {label}. Skipping.")
            self.conn.commit()
        except Exception as e:
            logging.error(f"Failed to insert stats data for PerfumeID {perfume_id}, Category {category}: {e}")
            self.conn.rollback()
        finally:
            self._close()

    def insert_reviews(self, perfume_id, reviews_list):
        self._connect()
        try:
            for review_data in reviews_list:
                # CHANGED: expecting new keys from scraper
                content = review_data.get('review_content')
                review_date = review_data.get('review_date')
                reviewer_name = review_data.get('reviewer_name')
                if content and isinstance(content, str):
                    # CHANGED: column names
                    self.cursor.execute(
                        "INSERT INTO Reviews (perfume_id, review_content, reviewer_name, review_date) VALUES (?, ?, ?, ?)",
                        (perfume_id, content, reviewer_name, review_date)
                    )
            self.conn.commit()
        except Exception as e:
            logging.error(f"Failed to insert reviews for PerfumeID {perfume_id}: {e}")
            self.conn.rollback()
        finally:
            self._close()

    def get_or_create_country(self, country_name, brand_count):
        self._connect()
        try:
            # CHANGED
            self.cursor.execute("SELECT country_id FROM Countries WHERE country_name = ?", country_name)
            result = self.cursor.fetchone()
            if result:
                return result[0]
            # CHANGED
            insert_query = "INSERT INTO Countries (country_name, brand_count) OUTPUT INSERTED.country_id VALUES (?, ?)"
            self.cursor.execute(insert_query, country_name, brand_count)
            new_id = self.cursor.fetchone()[0]
            self.conn.commit()
            return new_id
        except Exception as e:
            logging.error(f"Error in get_or_create_country for '{country_name}': {e}")
            self.conn.rollback()
            return None
        finally:
            self._close()

    # CHANGED: method signature and query
    def get_or_create_brand(self, brand_name, country_id, brand_url, perfume_count, brand_website_url, brand_image_url):
        self._connect()
        try:
            self.cursor.execute("SELECT id FROM Brands WHERE brand_name = ?", brand_name)
            result = self.cursor.fetchone()
            if result:
                return result[0]
            insert_query = """
                           INSERT INTO Brands (brand_name, country_id, brand_url, perfume_count, brand_website_url, \
                                               brand_image_url)
                               OUTPUT INSERTED.id
                           VALUES (?, ?, ?, ?, ?, ?) \
                           """
            self.cursor.execute(insert_query, brand_name, country_id, brand_url, perfume_count, brand_website_url,
                                brand_image_url)
            new_id = self.cursor.fetchone()[0]
            self.conn.commit()
            return new_id
        except Exception as e:
            logging.error(f"Error in get_or_create_brand for '{brand_name}': {e}")
            self.conn.rollback()
            return None
        finally:
            self._close()

    # CHANGED: method signature and queries
    def get_or_create_perfume(self, perfume_name, perfume_for, image_url, launch_year, perfumer_name, perfumer_url,
                              perfume_url, brand_id):
        self._connect()
        try:
            self.cursor.execute("SELECT perfume_id FROM Perfumes WHERE perfume_url = ?", perfume_url)
            existing = self.cursor.fetchone()
            year = int(launch_year) if str(launch_year).isdigit() else None

            if existing:
                perfume_id = existing[0]
                update_query = """
                               UPDATE Perfumes
                               SET perfume_name  = ?, \
                                   perfume_for   = ?, \
                                   image_url     = ?, \
                                   launch_year   = ?,
                                   perfumer_name = ?, \
                                   perfumer_url  = ?, \
                                   brand_id      = ?
                               WHERE perfume_id = ? \
                               """
                self.cursor.execute(update_query,
                                    (perfume_name, perfume_for, image_url, year, perfumer_name, perfumer_url, brand_id,
                                     perfume_id))
                self.conn.commit()
                return perfume_id

            insert_query = """
                           INSERT INTO Perfumes (perfume_name, perfume_for, image_url, launch_year, perfumer_name, \
                                                 perfumer_url, perfume_url, brand_id)
                               OUTPUT INSERTED.perfume_id
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
                           """
            self.cursor.execute(insert_query,
                                (perfume_name, perfume_for, image_url, year, perfumer_name, perfumer_url, perfume_url,
                                 brand_id))
            new_id = self.cursor.fetchone()[0]
            self.conn.commit()
            return new_id

        except Exception as e:
            logging.error(f"Error in get_or_create_perfume for '{perfume_name}': {e}")
            self.conn.rollback()
            return None
        finally:
            self._close()

    def get_or_create_id(self, table_name, column_name, value):
        self._connect()
        try:
            id_col, name_col = f"{column_name}_id", f"{column_name}_name"
            self.cursor.execute(f"SELECT {id_col} FROM {table_name} WHERE {name_col} = ?", value)
            res = self.cursor.fetchone()
            if res: return res[0]
            query = f"INSERT INTO {table_name} ({name_col}) OUTPUT INSERTED.{id_col} VALUES (?)"
            self.cursor.execute(query, value)
            new_id = self.cursor.fetchone()[0]
            self.conn.commit()
            return new_id
        except pyodbc.IntegrityError:
            self.conn.rollback()
            self.cursor.execute(f"SELECT {id_col} FROM {table_name} WHERE {name_col} = ?", value)
            return self.cursor.fetchone()[0]
        except Exception as e:
            logging.error(f"Error in get_or_create_id for {table_name}: {e}")
            self.conn.rollback()
            return None
        finally:
            self._close()

    # CHANGED: method signature and query
    def link_perfume_note(self, perfume_id, note_id, note_level):
        self._connect()
        try:
            self.cursor.execute("INSERT INTO PerfumeNotes (perfume_id, note_id, note_level) VALUES (?, ?, ?)",
                                perfume_id,
                                note_id, note_level)
            self.conn.commit()
        except pyodbc.IntegrityError:
            self.conn.rollback()
        finally:
            self._close()

    def link_perfume_accord(self, perfume_id, accord_id, accord_strength):
        self._connect()
        try:
            self.cursor.execute(
                "INSERT INTO PerfumeAccords (perfume_id, accord_id, accord_strength) VALUES (?, ?, ?)",
                (perfume_id, accord_id, accord_strength)
            )
            self.conn.commit()
        except pyodbc.IntegrityError:
            self.conn.rollback()
        finally:
            self._close()