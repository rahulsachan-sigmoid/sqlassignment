import psycopg2
import pandas as pd


def solution1(cur):
    q1 = "select e.empno,e.ename,m.ename from emp as e inner join emp as m on e.mgr=m.empno"
    cur.execute(q1)

    df = pd.DataFrame(cur.fetchall())

    df.to_excel('Solution1.xlsx', header=["Employee_Id", "Employee_Name", "Manager_Name"], index=False)


def solution2(cur):
    cur.execute(
        "select emp.ename,emp.empno, dept.dname ,(case when enddate is not null then (("
        "enddate-startdate+1)/30)*jobhist.sal else ((current_date-startdate+1)/30)*jobhist.sal end)as "
        "Total_Compensation, "
        "(case when enddate is not null then ((enddate-startdate+1)/30) else ((current_date-startdate+1)/30) end)as "
        "Months_Spent from jobhist, dept, emp "
        "where jobhist.deptno=dept.deptno and jobhist.empno=emp.empno")

    df2 = pd.DataFrame(cur.fetchall())

    df2.to_excel('total_compensation.xlsx',
                 header=["Emp_Name", "Emp_No", "Dept_Name", "Total_Compensation", "Months_Spent"], index=False)


def solution3(cur):
    cur.execute("COPY comp FROM '/Users/rahul/PycharmProjects/pythonProject1/sqlassignment/compensation.csv' "
                "DELIMITER ',' CSV HEADER;")


def solution4(cur):
    cur.execute(
        "select dept.deptno, Dept_Name, sum(Total_Compensation) from comp, dept where Comp.dept_Name=dept.dname group "
        "by Dept_Name, dept.deptno")
    df4 = pd.DataFrame(cur.fetchall())
    df4.to_excel('Department_compensation.xlsx', header=["Dept No", "Dept Name", "Compensation"], index=False)


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect("dbname=rahul user=postgres password=Rahul@7785 host=localhost port=5433")

        # create a cursor
        cur = conn.cursor()

        # solution to ques1
        solution1(cur);

        # solution to ques2
        solution2(cur);

        # solution to ques4
        # solution3(cur);

        # solution to ques4
        solution4(cur);

        conn.commit()

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    connect()
