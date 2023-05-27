from pprint import pprint
from sqlalchemy import func, desc, select, and_

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_one():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    :return: list[dict]
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_two(discipline_id: int):
    r = session.query(Discipline.name,
                      Student.fullname,
                      func.round(func.avg(Grade.grade), 2).label('avg_grade')
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Student.id, Discipline.name) \
        .order_by(desc('avg_grade')) \
        .limit(1).all()
    return r

def select_three(discipline_id):
    '''Найти средний балл в группах по определенному предмету.'''
    result = session.query(Group.name, func.round(func.avg(Grade.grade), 2), Discipline.name)\
                    .select_from(Grade)\
                    .join(Student)\
                    .join(Group)\
                    .join(Discipline)\
                    .filter(Discipline.id == discipline_id)\
                    .group_by(Group.name, Discipline.name)\
                    .all()
    return result
    
def select_four():
    '''Найти средний балл на потоке (по всей таблице оценок).'''

    result = session.query(func.round(func.avg(Grade.grade),2))\
                    .select_from(Grade)\
                    .all()
    return result
    
def select_five(teacher_id):
    """Найти какие курсы читает определенный преподаватель."""
    result = session.query(Discipline.name, Teacher.fullname)\
                    .select_from(Grade)\
                    .filter(Teacher.id == teacher_id)\
                    .group_by(Discipline.name, Teacher.fullname)\
                    .all()
    return result

def select_six(group_id):
        """Найти список студентов в определенной группе."""
        result = session.query(Student.fullname, Group.name)\
                        .select_from(Student)\
                        .join(Group)\
                        .filter(Group.id == group_id)\
                        .all()
        return result

def select_seven(group_id, discipline_id):
     """Найти оценки студентов в отдельной группе по определенному предмету."""
     result = session.query(Grade.grade, Student.fullname, Discipline.name)\
                     .select_from(Grade)\
                     .join(Student)\
                     .join(Discipline)\
                     .filter(and_(Discipline.id == discipline_id, Group.id == group_id))\
                     .order_by(desc(Student.fullname))\
                     .limit(20).all()
     return result

def select_eight(teacher_id):
     """Найти средний балл, который ставит определенный преподаватель по своим предметам."""
     result = session.query(Discipline.name, func.round(func.avg(Grade.grade), 2), Teacher.fullname)\
                     .select_from(Grade)\
                     .join(Discipline)\
                     .join(Teacher)\
                     .filter(Teacher.id == teacher_id)\
                     .group_by(Discipline.name, Teacher.fullname)\
                     .all()
    
     return result

def select_nine(student_id):
     """Найти список курсов, которые посещает определенный студент."""
     result = session.query(Discipline.name, Student.fullname)\
                     .select_from(Grade)\
                     .join(Student)\
                     .join(Discipline)\
                     .filter(Student.id == student_id)\
                     .group_by(Discipline.name, Student.fullname)\
                     .all()
     return result 

def select_ten(teacher_id, student_id):
     """Список курсов, которые определенному студенту читает определенный преподаватель."""
     result = session.query(Discipline.name, Student.fullname, Teacher.fullname)\
                     .select_from(Grade)\
                     .join(Discipline)\
                     .join(Student)\
                     .join(Teacher)\
                     .filter(and_(Student.id == student_id, Teacher.id == teacher_id))\
                     .group_by(Discipline.name, Student.fullname, Teacher.fullname)\
                     .all()
     return result
     
def select_eleven(teacher_id, student_id):
     """Средний балл, который определенный преподаватель ставит определенному студенту."""
     result = session.query(func.round(func.avg(Grade.grade), 2), Teacher.fullname, Student.fullname)\
                     .select_from(Grade)\
                     .join(Student)\
                     .join(Discipline)\
                     .join(Teacher)\
                     .filter(and_(Student.id == student_id, Teacher.id == teacher_id))\
                     .group_by(Student.fullname, Teacher.fullname)\
                     .all()
     return result


def select_twelve(discipline_id, group_id):
    """Оценки студентов в определенной группе по определенному предмету на последнем занятии."""
    subquery = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == discipline_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1).scalar_subquery())

    r = session.query(Discipline.name,
                      Student.fullname,
                      Group.name,
                      Grade.date_of,
                      Grade.grade
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group)\
        .filter(and_(Discipline.id == discipline_id, Group.id == group_id, Grade.date_of == subquery)) \
        .order_by(desc(Grade.date_of)) \
        .all()
    return r


if __name__ == '__main__':
    pprint(select_one())
    pprint(select_two(1))
    pprint(select_three(5))
    pprint(select_four())
    pprint(select_five(1))
    pprint(select_six(2))
    pprint(select_seven(1, 2))
    pprint(select_eight(2))
    pprint(select_nine(1))
    pprint(select_ten(1, 2))
    pprint(select_eleven(1, 2))
    pprint(select_twelve(1, 2))
