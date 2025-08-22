## Setup Instructions

1. **Update Configuration**
   - Open `config.py` and update your server name as needed.

2. **Database Setup**
   - Create a new database named **`FragranceDB`**.

3. **Install Dependencies**
   - Run the following command in your terminal to install all required dependencies:
     ```bash
     pip install -r requirements.txt
     ```

4. **Data Import**
   - First, run `import_brands_data.py` to insert brands and countries data into the database tables.
   - Then, run `main.py` to import the rest of the data.
