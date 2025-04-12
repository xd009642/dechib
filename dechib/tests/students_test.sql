-- Drop tables if they already exist to avoid errors
DROP TABLE IF EXISTS Enrollments;
DROP TABLE IF EXISTS Lessons;
DROP TABLE IF EXISTS Students;
DROP TABLE IF EXISTS Professors;

-- Create Students table
CREATE TABLE Students (
    student_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    date_of_birth DATE,
    enrollment_date DATE
);

-- Create Professors table
CREATE TABLE Professors (
    professor_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    hire_date DATE,
    department VARCHAR(100)
);

-- Create Lessons table
CREATE TABLE Lessons (
    lesson_id INT PRIMARY KEY,
    lesson_name VARCHAR(100),
    credits INT,
    professor_id INT,
    FOREIGN KEY (professor_id) REFERENCES Professors(professor_id)
);

-- Create Enrollments table
CREATE TABLE Enrollments (
    enrollment_id INT PRIMARY KEY,
    student_id INT,
    lesson_id INT,
    enrollment_date DATE,
    grade VARCHAR(2),
    FOREIGN KEY (student_id) REFERENCES Students(student_id),
    FOREIGN KEY (lesson_id) REFERENCES Lessons(lesson_id)
);

-- Insert data into Students
INSERT INTO Students (student_id, first_name, last_name, email, date_of_birth, enrollment_date) VALUES
(1, 'Alice', 'Johnson', 'alice.johnson@example.com', '2002-03-15', '2021-09-01'),
(2, 'Bob', 'Smith', 'bob.smith@example.com', '2001-07-22', '2020-09-01'),
(3, 'Charlie', 'Brown', 'charlie.brown@example.com', '2003-11-30', '2022-09-01'),
(4, 'Dana', 'White', 'dana.white@example.com', '2000-12-05', '2019-09-01');

-- Insert data into Professors
INSERT INTO Professors (professor_id, first_name, last_name, email, hire_date, department) VALUES
(1, 'Dr. Emma', 'Taylor', 'emma.taylor@university.edu', '2015-08-15', 'Computer Science'),
(2, 'Dr. Liam', 'Anderson', 'liam.anderson@university.edu', '2010-01-10', 'Mathematics'),
(3, 'Dr. Olivia', 'Martinez', 'olivia.martinez@university.edu', '2018-05-20', 'Physics');

-- Insert data into Lessons
INSERT INTO Lessons (lesson_id, lesson_name, credits, professor_id) VALUES
(101, 'Intro to Programming', 4, 1),
(102, 'Calculus I', 3, 2),
(103, 'Physics I', 4, 3),
(104, 'Data Structures', 3, 1),
(105, 'Linear Algebra', 3, 2);

-- Insert data into Enrollments
INSERT INTO Enrollments (enrollment_id, student_id, lesson_id, enrollment_date, grade) VALUES
(1, 1, 101, '2021-09-01', 'A'),
(2, 1, 102, '2021-09-01', 'B'),
(3, 2, 101, '2020-09-01', 'C'),
(4, 2, 103, '2020-09-01', 'B'),
(5, 3, 104, '2022-09-01', NULL),
(6, 3, 102, '2022-09-01', NULL),
(7, 4, 105, '2019-09-01', 'A'),
(8, 4, 103, '2019-09-01', 'A');

