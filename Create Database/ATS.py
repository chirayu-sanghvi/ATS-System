import psycopg2
from faker import Faker
import random

# Function to establish connection to PostgreSQL
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname="ATS",
            user="<your username>",
            password="<your password>",
            host="localhost",
            port="5432"
        )
        print("Connected to the database successfully!")
        return conn
    except psycopg2.Error as e:
        print("Unable to connect to the database.")
        print(e)
        return None

def generate_job_description(field):
    descriptions = {
        'Engineering': 'Responsible for developing and designing engineering systems, must have a solid understanding of physics, mathematics, and software development.',
        'Human Resource': 'Oversees employee relations, payroll, benefits, and training. Must have excellent communication skills and understanding of labor laws.',
        'Finance': 'Manages the company\'s financial planning, risk management, and accounting practices. Knowledge of financial statutes and market analysis is essential.',
        'Education': 'Designs and implements educational programs and curricula. Must have a deep understanding of teaching methodologies and student engagement strategies.'
    }
    return descriptions.get(field, 'General job responsibilities not defined.')

# Function to generate a unique 6-digit application ID
def generate_unique_application_id(existing_ids):
    while True:
        application_id = random.randint(100000, 999999)  # Generate a 6-digit number
        if application_id not in existing_ids:
            return application_id

def generate_unique_interview_id(existing_ids):
    while True:
        interviewer_id = random.randint(10000, 99999)  # Generate a 5-digit number
        if interviewer_id not in existing_ids:
            return interviewer_id

# Function to create tables
def create_tables(conn):
    cur = conn.cursor()
    try:
        # Create Applicant table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Applicant (
            username VARCHAR PRIMARY KEY,
            legalName VARCHAR,
            password VARCHAR,
            email VARCHAR,
            Address VARCHAR,
            field VARCHAR,
            skills VARCHAR,
            yearOfExperience INTEGER,
            workPreference VARCHAR
        );
        """)
        # Create Companies table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Companies (
            companyId VARCHAR PRIMARY KEY,
            password VARCHAR,
            companyName VARCHAR,
            location VARCHAR,
            companyEmail VARCHAR
        );
        """)
        # Create Jobs table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Jobs (
            jobId SERIAL PRIMARY KEY,
            companyId VARCHAR REFERENCES Companies(companyId),
            jobTitle VARCHAR,
            jobDescription VARCHAR,
            requiredYOE VARCHAR,
            requiredSkills VARCHAR,
            requiredWorkPreference VARCHAR,
            requiredField VARCHAR,
            postedDate DATE,
            isActive BOOLEAN
        );
        """)
        # Create Applications table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Applications (
            applicationId SERIAL PRIMARY KEY,
            username VARCHAR REFERENCES Applicant(username),
            jobId INTEGER REFERENCES Jobs(jobId),
            isShortlisted Boolean,
            currentStatus VARCHAR,
            UNIQUE(username, jobId)
        );
        """)
        # Create Interviewer table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Interviewer (
            InterviewId SERIAL PRIMARY KEY,
            companyId VARCHAR REFERENCES Companies(companyId),
            interviewerName VARCHAR,
            fieldExpertise VARCHAR
        );
        """)
        # Create Interview table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Interview (
            applicationId INTEGER REFERENCES Applications(applicationId),
            interviewerId INTEGER REFERENCES Interviewer(InterviewId),
            applicantLegalName VARCHAR,
            interviewerName VARCHAR,
            isSelected INTEGER,
            PRIMARY KEY (applicationId, interviewerId)
        );
        """)
        conn.commit()
        print("Tables created successfully!")
    except psycopg2.Error as e:
        print("Error creating tables.")
        print(e)

# Function to generate and insert minimum required tuples into tables
def generate_and_insert_data(conn, min_applicants=5000, min_companies=40, min_jobs_per_company=10, min_interviewers_per_company=2):
    cur = conn.cursor()
    faker = Faker()

    existing_usernames = set()
    cur.execute("SELECT username FROM Applicant")
    for row in cur.fetchall():
        existing_usernames.add(row[0])
    
    while len(existing_usernames) < min_applicants:
        username = faker.unique.user_name()
        if username in existing_usernames:
            continue
        existing_usernames.add(username)

        legal_name = faker.name()
        password = faker.password()
        email = faker.email()
        address = faker.address().replace('\n', ', ')
        field = random.choice(['Engineering', 'Human Resource', 'Finance', 'Education'])
        # Conditional logic to set skills based on the field
        if field == 'Engineering':
            skills = random.choice(['C++ and Agile', 'Java and Software Development life cycle', 'Python and Automation testing', 'Javascript and Clound computing'])
        elif field == 'Human Resource':
            skills = random.choice(['Strategic thinking and LinkedIn recruiter', 'Email Writing and Collaborative', 'Strong Communication and Jira', 'Interpersonal and Human resource information'])
        elif field == 'Finance':
            skills = random.choice(['Accounting and Financial modeling', 'Data Analysis and Budgeting', 'SAP and cash flow management', 'MS Office and Risk analysis'])
        else:  # Education
            skills = random.choice(['Mathematics and Curriculum Expertise', 'Computer Science and Critical thinking', 'Physics and Research abilities', 'Pharmacy and Lab experience'])
        year_of_experience = random.randint(1, 10)
        work_preference = random.choice(['Hybrid', 'Remote', 'Onsite'])
        cur.execute("""
        INSERT INTO Applicant (username, legalName, password, email, Address, field, skills, yearOfExperience, workPreference) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (username, legal_name, password, email, address, field, skills, year_of_experience, work_preference))

    # Initialize a set to track existing company names
    existing_company_names = set()
    cur.execute("SELECT companyName FROM Companies")
    for row in cur.fetchall():
        existing_company_names.add(row[0].lower())  # Use lower case to ensure case-insensitive comparison

    for _ in range(min_companies):
        while True:
            company_name = faker.company()
            # Ensure the generated company name is unique
            if company_name.lower() not in existing_company_names:
                break
        
        existing_company_names.add(company_name.lower())  # Add the new company name to the set
        company_id = faker.unique.lexify(text="???").upper()
        password = faker.password()
        location = faker.city() + ", " + faker.state()
        company_email = faker.company_email()
        cur.execute("""
        INSERT INTO Companies (companyId, password, companyName, location, companyEmail) 
        VALUES (%s, %s, %s, %s, %s)
        """, (company_id, password, company_name, location, company_email))

    cur.execute("SELECT companyId FROM Companies")
    company_ids = [record[0] for record in cur.fetchall()]
    for company_id in company_ids:
        for _ in range(random.randint(min_jobs_per_company, min_jobs_per_company + 2)):
            job_title = faker.job()
            required_yoe = str(random.randint(1, 10))
            required_field = random.choice(['Engineering', 'Human Resource', 'Finance', 'Education'])
            
            # Conditional logic to set required_skills based on the required_field
            if required_field == 'Engineering':
                required_skills = random.choice(['C++ and Agile', 'Java and Software Development life cycle', 'Python and Automation testing', 'Javascript and Cloud computing'])
                job_title = random.choice(['Software Engineer','Member of Technical Staff'])
            elif required_field == 'Human Resource':
                required_skills = random.choice(['Strategic thinking and LinkedIn recruiter', 'Email Writing and Collaborative', 'Strong Communication and Jira', 'Interpersonal and Human resource information'])
                job_title = random.choice(['HR Manager', 'Talent Acquisition Specialist', 'Employee Relations Specialist', 'Compensation and Benefits Manager'])
            elif required_field == 'Finance':
                required_skills = random.choice(['Accounting and Financial modeling', 'Data Analysis and Budgeting', 'SAP and cash flow management', 'MS Office and Risk analysis'])
                job_title = random.choice(['Financial Analyst', 'Accountant', 'Investment Banker', 'Charted Financial Consultant'])
            else:
                required_skills = random.choice(['Mathematics and Curriculum Expertise', 'Computer Science and Critical thinking', 'Physics and Research abilities', 'Pharmacy and Lab experience'])
                job_title = random.choice(['Subject Professor', 'Curricullum Developer', 'Educational Consultant', 'STEM Instructor'])

            job_description = generate_job_description(required_field)
            required_work_preference = random.choice(['Hybrid', 'Remote', 'Onsite'])
            posted_date = faker.date_between(start_date="-1y", end_date="today")
            is_active = faker.boolean(chance_of_getting_true=80)
            cur.execute("""
            INSERT INTO Jobs (companyId, jobTitle, jobDescription, requiredYOE, requiredSkills, requiredWorkPreference, requiredField, postedDate, isActive) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (company_id, job_title, job_description, required_yoe, required_skills, required_work_preference, required_field, posted_date, is_active))

# Assuming cur has been defined and connected to your database
    existing_application_ids = set()

    cur.execute("""
    SELECT jobId, requiredYOE, requiredField, requiredSkills, requiredWorkPreference FROM Jobs where isActive= 'true'
    """)
    jobs = cur.fetchall()

    for job in jobs:
        job_id, required_yoe, required_field, required_skills, required_work_preference = job
        
        cur.execute("""
        SELECT username FROM Applicant
        WHERE 
            ABS(yearOfExperience - %s) <= 1 AND
            field = %s AND
            skills = %s AND
            workPreference = %s
        """, (required_yoe, required_field, required_skills, required_work_preference))
        
        matching_applicants = [row[0] for row in cur.fetchall()]
        
        for username in matching_applicants:
            application_id = generate_unique_application_id(existing_application_ids)
            existing_application_ids.add(application_id)  # Keep track of used IDs
            
            current_status = 'Applied'
            isShortlisted = random.choices([True, False], weights=[70, 30], k=1)[0]
            cur.execute("""
            INSERT INTO Applications (applicationId, username, jobId, isShortlisted,currentStatus) 
            VALUES (%s, %s, %s, %s, %s)
            """, (application_id, username, job_id, isShortlisted, current_status))

    field_expertise_options = ['Engineering', 'Human Resource', 'Finance', 'Education']
    unique_interviewer_names = set()  # To track unique names
    existing_interviewer_ids = set()
    # Assuming company_ids and cur have been defined earlier
    for company_id in company_ids:
        for field_expertise in field_expertise_options:
            # Generate a unique name for each interviewer
            while True:
                interviewer_name = faker.unique.name()
                if interviewer_name not in unique_interviewer_names:
                    unique_interviewer_names.add(interviewer_name)
                    break
            interviewer_id = generate_unique_interview_id(existing_interviewer_ids)
            existing_interviewer_ids.add(interviewer_id)  # Keep track of used IDs
            # Insert an interviewer for each field of expertise for the current company
            cur.execute("""
            INSERT INTO Interviewer (InterviewId, companyId, interviewerName, fieldExpertise) 
            VALUES (%s, %s, %s, %s)
            """, (interviewer_id,company_id, interviewer_name, field_expertise))

    # Fetch all interviewing applications
    cur.execute("SELECT applicationId, jobId FROM Applications WHERE currentStatus = 'Applied' AND isShortlisted = TRUE")
    interviewing_applications = cur.fetchall()

    for application_id, job_id in interviewing_applications:
        # Fetch the applicant's legal name
        cur.execute("""
        SELECT legalName FROM Applicant 
        WHERE username = (
            SELECT username FROM Applications WHERE applicationId = %s
        )
        """, (application_id,))
        applicant_legal_name = cur.fetchone()[0]

        # Fetch the required field of expertise and company ID for the job
        cur.execute("""
        SELECT requiredField, companyId FROM Jobs WHERE jobId = %s
        """, (job_id,))
        required_field, company_id = cur.fetchone()

        # Fetch the interviewer ID and name based on the company ID and field of expertise
        cur.execute("""
        SELECT InterviewId, interviewerName FROM Interviewer 
        WHERE companyId = %s AND fieldExpertise = %s
        """, (company_id, required_field))
        result = cur.fetchone()
        if result:
            interviewer_id, interviewer_name = result
            isSelected = random.randint(0, 2)  # Randomly choose between 0, 1, and 2

            # Insert the data into the Interview table including applicantLegalName and interviewerName
            cur.execute("""
            INSERT INTO Interview (applicationId, interviewerId, applicantLegalName, interviewerName, isSelected) 
            VALUES (%s, %s, %s, %s, %s)
            """, (application_id, interviewer_id, applicant_legal_name, interviewer_name, isSelected))

            # Update the currentStatus in the Applications table based on isSelected value
            if isSelected == 0:
                new_status = 'Rejected'
            elif isSelected == 1:
                new_status = 'Interviewing'  # Or consider another status if needed since they were already interviewing
            elif isSelected == 2:
                new_status = 'Hired'

            cur.execute("""
            UPDATE Applications SET currentStatus = %s WHERE applicationId = %s
            """, (new_status, application_id))

    conn.commit()
    print("Data generation and insertion complete.")

if __name__ == "__main__":
    conn = connect_to_db()
    if conn:
        create_tables(conn)
        generate_and_insert_data(conn, 5000, 40, 10, 2)


