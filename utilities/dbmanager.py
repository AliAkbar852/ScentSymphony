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
                Id INT IDENTITY(1,1) PRIMARY KEY,
                country_name NVARCHAR(255) UNIQUE NOT NULL,
                brand_count INT
            )''')

            # 2. Brands
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.Brands', N'U') IS NULL
            CREATE TABLE Brands (
                Id INT IDENTITY(1,1) PRIMARY KEY,
                name NVARCHAR(255) UNIQUE NOT NULL,
                countryID INT FOREIGN KEY REFERENCES Countries(Id),
                BrandUrl NVARCHAR(500),
                PerfumeCount INT,
                WebsiteUrl NVARCHAR(500),
                imageUrl NVARCHAR(500)
            )''')

            # 3. Perfumes
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.Perfumes', N'U') IS NULL
            CREATE TABLE Perfumes (
                PerfumeID INT IDENTITY(1,1) PRIMARY KEY,
                name NVARCHAR(255),
                subtitle NVARCHAR(255),
                image_url NVARCHAR(500),
                launch_year INT,
                perfumer_name NVARCHAR(255),
                perfumer_url NVARCHAR(500),
                url NVARCHAR(500) UNIQUE NOT NULL,
                BrandID INT NULL FOREIGN KEY REFERENCES Brands(Id)
            )''')

            # 4. Notes
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.notes', N'U') IS NULL
            CREATE TABLE notes (
                    note_id INT IDENTITY(1,1) PRIMARY KEY,
                note_name NVARCHAR(255) UNIQUE NOT NULL
            )''')

            # 5. Accords
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.accords', N'U') IS NULL
            CREATE TABLE accords (
                accord_id INT IDENTITY(1,1) PRIMARY KEY,
                accord_name NVARCHAR(255) UNIQUE NOT NULL
            )''')

            # 6. PerfumeNotes
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.perfume_notes', N'U') IS NULL
            CREATE TABLE perfume_notes (
                perfume_id INT FOREIGN KEY REFERENCES Perfumes(PerfumeID) ON DELETE CASCADE,
                note_id INT FOREIGN KEY REFERENCES notes(note_id) ON DELETE CASCADE,
                level NVARCHAR(50),
                PRIMARY KEY (perfume_id, note_id, level)
            )''')

            # 7. PerfumeAccords
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.perfume_accords', N'U') IS NULL
            CREATE TABLE perfume_accords (
                perfume_id INT FOREIGN KEY REFERENCES Perfumes(PerfumeID) ON DELETE CASCADE,
                accord_id INT FOREIGN KEY REFERENCES accords(accord_id) ON DELETE CASCADE,
                accord_strength DECIMAL(5,2) NULL,
                PRIMARY KEY (perfume_id, accord_id)
            )''')

            # 8. PerfumeVotes
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.PerfumeVotes', N'U') IS NULL
            CREATE TABLE PerfumeVotes (
                VoteID INT IDENTITY(1,1) PRIMARY KEY,
                PerfumeID INT NOT NULL,
                ReviewCount INT NOT NULL DEFAULT 0,
                RatingCount INT NOT NULL DEFAULT 0,
                RatingValue DECIMAL(3, 2) NOT NULL,
                FOREIGN KEY (PerfumeID) REFERENCES Perfumes(PerfumeID) ON DELETE CASCADE
            )''')

            # 9. PerfumePercentages
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.PerfumePercentages', N'U') IS NULL
            CREATE TABLE PerfumePercentages (
                PercentageID INT IDENTITY(1,1) PRIMARY KEY,
                PerfumeID INT NOT NULL,
                Category VARCHAR(50) NOT NULL,
                Label VARCHAR(50) NOT NULL,
                PercentageValue DECIMAL(5, 2) NOT NULL,
                FOREIGN KEY (PerfumeID) REFERENCES Perfumes(PerfumeID) ON DELETE CASCADE
            )''')

            # 10. PerfumeStats
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.PerfumeStats', N'U') IS NULL
            CREATE TABLE PerfumeStats (
                StatID INT IDENTITY(1,1) PRIMARY KEY,
                PerfumeID INT NOT NULL,
                Category VARCHAR(50) NOT NULL,
                Label VARCHAR(50) NOT NULL,
                VoteCount INT NOT NULL DEFAULT 0,
                FOREIGN KEY (PerfumeID) REFERENCES Perfumes(PerfumeID) ON DELETE CASCADE
            )''')

            # 11. Reviews
            self.cursor.execute('''
            IF OBJECT_ID(N'dbo.Reviews', N'U') IS NULL
            CREATE TABLE Reviews (
                ReviewID INT IDENTITY(1,1) PRIMARY KEY,
                PerfumeID INT NOT NULL,
                Content NVARCHAR(MAX),
                UserName NVARCHAR(250),
                DatePublished DATE,
                FOREIGN KEY (PerfumeID) REFERENCES Perfumes(PerfumeID) ON DELETE CASCADE
            )''')

            self.conn.commit()
            logging.info("All tables checked/created successfully.")

        except pyodbc.Error as e:
            logging.error(f"Error creating tables: {e}")
            self.conn.rollback()
        finally:
            self._close()

    def clear_perfume_details(self, perfume_id):
        """Deletes old votes, stats, percentages, and reviews for a perfume before inserting new ones."""
        self._connect()
        try:
            logging.debug(f"Clearing old detail data for PerfumeID: {perfume_id}")
            self.cursor.execute("DELETE FROM PerfumeVotes WHERE PerfumeID = ?", perfume_id)
            self.cursor.execute("DELETE FROM PerfumePercentages WHERE PerfumeID = ?", perfume_id)
            self.cursor.execute("DELETE FROM PerfumeStats WHERE PerfumeID = ?", perfume_id)
            self.cursor.execute("DELETE FROM Reviews WHERE PerfumeID = ?", perfume_id)
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error clearing details for PerfumeID {perfume_id}: {e}")
            self.conn.rollback()
        finally:
            self._close()

    def insert_perfume_vote(self, perfume_id, data):
        """Inserts the main review/rating counts and value for a perfume."""
        self._connect()
        try:
            self.cursor.execute("""
                                INSERT INTO PerfumeVotes (PerfumeID, ReviewCount, RatingCount, RatingValue)
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
        """Inserts a dictionary of percentage data for a given category."""
        self._connect()
        try:
            for label, percent_str in data_dict.items():
                try:
                    percentage = float(str(percent_str).strip('%'))
                    self.cursor.execute("""
                                        INSERT INTO PerfumePercentages (PerfumeID, Category, Label, PercentageValue)
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
        """Inserts a dictionary of statistical vote data for a given category."""
        self._connect()
        try:
            for label, votes in data_dict.items():
                try:
                    self.cursor.execute("""
                                        INSERT INTO PerfumeStats (PerfumeID, Category, Label, VoteCount)
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
        """Inserts a list of review texts and dates for a perfume."""
        self._connect()
        try:
            for review_data in reviews_list:
                content = review_data.get('review_text')
                date_published = review_data.get('review_date')
                username = review_data.get('UserName')
                if content and isinstance(content, str):
                    self.cursor.execute(
                        "INSERT INTO Reviews (PerfumeID, Content, UserName, DatePublished) VALUES (?,?, ?, ?)",
                        (perfume_id, content, username, date_published)
                    )
            self.conn.commit()
        except Exception as e:
            logging.error(f"Failed to insert reviews for PerfumeID {perfume_id}: {e}")
            self.conn.rollback()
        finally:
            self._close()
        # --- Methods for Countries and Brands ---

    def get_or_create_country(self, country_name, brand_count):
        self._connect()
        try:
            self.cursor.execute("SELECT Id FROM Countries WHERE country_name = ?", country_name)
            result = self.cursor.fetchone()
            if result:
                return result[0]
            insert_query = "INSERT INTO Countries (country_name, brand_count) OUTPUT INSERTED.Id VALUES (?, ?)"
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

    def get_or_create_brand(self, name, countryID, BrandUrl, PerfumeCount, WebsiteUrl, imageUrl):
        self._connect()
        try:
            self.cursor.execute("SELECT Id FROM Brands WHERE name = ?", name)
            result = self.cursor.fetchone()
            if result:
                return result[0]
            insert_query = "INSERT INTO Brands (name, countryID, BrandUrl, PerfumeCount, WebsiteUrl, imageUrl) OUTPUT INSERTED.Id VALUES (?, ?, ?, ?, ?, ?)"
            self.cursor.execute(insert_query, name, countryID, BrandUrl, PerfumeCount, WebsiteUrl, imageUrl)
            new_id = self.cursor.fetchone()[0]
            self.conn.commit()
            return new_id
        except Exception as e:
            logging.error(f"Error in get_or_create_brand for '{name}': {e}")
            self.conn.rollback()
            return None
        finally:
            self._close()

    # --- All other existing methods (get_or_create_perfume, etc.) remain unchanged ---
    def get_or_create_perfume(self, name, subtitle, image_url, launch_year, perfumer_name, perfumer_url, url, brand_id):
        self._connect()
        try:
            self.cursor.execute("SELECT PerfumeID FROM Perfumes WHERE url = ?", url)
            existing = self.cursor.fetchone()
            year = int(launch_year) if str(launch_year).isdigit() else None

            if existing:
                perfume_id = existing[0]
                # ✅ Update details to fix missing values on reruns
                update_query = """
                    UPDATE Perfumes
                    SET name = ?, subtitle = ?, image_url = ?, launch_year = ?, 
                        perfumer_name = ?, perfumer_url = ?, BrandID = ?
                    WHERE PerfumeID = ?
                """
                self.cursor.execute(update_query, (name, subtitle, image_url, year, perfumer_name, perfumer_url, brand_id, perfume_id))
                self.conn.commit()
                return perfume_id

        # Insert if not exists
            insert_query = """
                INSERT INTO Perfumes (name, subtitle, image_url, launch_year, perfumer_name, perfumer_url, url, BrandID)
                OUTPUT INSERTED.PerfumeID
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            self.cursor.execute(insert_query, (name, subtitle, image_url, year, perfumer_name, perfumer_url, url, brand_id))
            new_id = self.cursor.fetchone()[0]
            self.conn.commit()
            return new_id

        except Exception as e:
            logging.error(f"Error in get_or_create_perfume for '{name}': {e}")
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

    def link_perfume_note(self, perfume_id, note_id, level):
        self._connect()
        try:
            self.cursor.execute("INSERT INTO perfume_notes (perfume_id, note_id, level) VALUES (?, ?, ?)", perfume_id,
                                note_id, level)
            self.conn.commit()
        except pyodbc.IntegrityError:
            self.conn.rollback()
        finally:
            self._close()

    def link_perfume_accord(self, perfume_id, accord_id, accord_strength):
        self._connect()
        try:
            self.cursor.execute(
            "INSERT INTO perfume_accords (perfume_id, accord_id, accord_strength) VALUES (?, ?, ?)",
            (perfume_id, accord_id, accord_strength)
        )
            self.conn.commit()
        except pyodbc.IntegrityError:
            self.conn.rollback()
        finally:
            self._close()