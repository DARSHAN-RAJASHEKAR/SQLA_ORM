from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, func
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# Setup for DB
engine = create_engine("sqlite:///rands1.db", echo=True)
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

#Table1
class Intern(Base):
    __tablename__ = 'interns'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    age = Column(Integer)
    address = Column(String)
    email = Column(String)
    projects = relationship("Project", back_populates="intern")

#Table2
class Project(Base):
    __tablename__ = 'works'
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_name = Column(String)
    intern_id = Column(Integer, ForeignKey('interns.id'))
    intern = relationship("Intern", back_populates="projects")

# Create tables
Base.metadata.create_all(engine)

# Insert data
new_interns = [
    Intern(name='Darshan', age=22, address='Lalbagh Road', email='d@email.com'),
    Intern(name='Prajwal', age=23, address='BTM', email='p@email.com'),
    Intern(name='Shivu', age=24, address='Nagarbhavi', email='s@email.com'),
    Intern(name='Sanjay', age=22, address='BTM', email='s@email.com'),
    Intern(name='Mayur', age=22, address='DoddaKallasandra', email='m@email.com')
]
new_projects = [
    Project(project_name='Schreiber Foods', intern_id=1),
    Project(project_name='Learning', intern_id=1),
    Project(project_name='Schreiber Foods', intern_id=2),
    Project(project_name='Schreiber Foods', intern_id=3),
    Project(project_name='Learning', intern_id=3),
    Project(project_name='Schreiber Foods', intern_id=4)
]
session.add_all(new_interns + new_projects)
session.commit()


# Queries
# Display all records from table1
all_interns = session.query(Intern).all()
for intern in all_interns:
    print(intern.name, intern.age, intern.address, intern.email)

# Display all records from table2
all_projects = session.query(Project).all()
for project in all_projects:
    print(project.project_name, project.intern_id)


#Joins
# Inner Join (Matching values in both tables)
for intern_name, project_name in session.query(Intern.name, Project.project_name).join(Project):
    print(intern_name, project_name)

#  Left Join (If no match, return NULL on the side of the right table)
for intern, project in session.query(Intern, Project).outerjoin(Project):
    print(intern.name, project.project_name if project else None)

# Update address of a specific intern
session.query(Intern).filter(Intern.name == 'Darshan').update({"address": "New Address"})
session.commit()

# Verify Update
updated_intern = session.query(Intern).filter_by(name='Darshan').first()
print(f"\nUpdated Address for Darshan: {updated_intern.address}")

#Delete a specific intern
session.query(Intern).filter(Intern.name == 'Darshan').delete()
session.commit()

# Verify Delete
remaining_interns = session.query(Intern).all()
for intern in remaining_interns:
    print(intern.name)

#Subqueries
subquery_result = session.query(Intern.name).filter(
    Intern.id.in_(
        session.query(Project.intern_id).filter_by(project_name='Schreiber Foods')
    )
)
for name in subquery_result:
    print(name[0])

# Nested Subquery
#Nested - subquery that appears within another subquery
nested_subquery_result = session.query(Intern.name).filter(
    Intern.id.in_(
        session.query(Project.intern_id).filter(
            Project.project_name.like('%Foods%')
        )
    )
)
for name in nested_subquery_result:
    print(name[0])

# Aggregate Functions
# Count, Average, Min, Max
total_interns = session.query(func.count(Intern.id)).scalar()
average_age = session.query(func.avg(Intern.age)).scalar()
minimum_age = session.query(func.min(Intern.age)).scalar()
maximum_age = session.query(func.max(Intern.age)).scalar()
print(f"\nAggregate Information - Total Interns: {total_interns}, Avg Age: {average_age}, Min Age: {minimum_age}, Max Age: {maximum_age}")

#Order By
# Ascending
for intern in session.query(Intern).order_by(Intern.age.asc()):
    print(intern.name, intern.age)

#Descending
for intern in session.query(Intern).order_by(Intern.age.desc()):
    print(intern.name, intern.age)

# Group By
average_age_per_project = session.query(
    Project.project_name, func.avg(Intern.age).label("average_age")
).join(Intern).group_by(Project.project_name)

for project_name, average_age in average_age_per_project:
    print(f"Project: {project_name}, Average Age of Interns: {average_age}")