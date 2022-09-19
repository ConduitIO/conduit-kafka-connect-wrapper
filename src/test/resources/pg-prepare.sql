DROP TABLE IF EXISTS employees;
DROP SEQUENCE IF EXISTS employees_id_seq;
CREATE TABLE employees (
    id int NOT NULL,
    name varchar(255),
    full_time bool,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (id)
);
CREATE SEQUENCE employees_id_seq;
ALTER TABLE employees ALTER id SET DEFAULT NEXTVAL('employees_id_seq');
ALTER TABLE employees REPLICA IDENTITY FULL;
