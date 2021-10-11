from sqlalchemy import create_engine
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Integer, String, Column, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import ForeignKey, func, desc, and_
from sqlalchemy.orm import backref, relationship

Base = declarative_base()

class Sailor(Base):
    __tablename__ = 'sailors'
    sid = Column(Integer, primary_key=True)
    sname = Column(String)
    rating = Column(Integer)
    age = Column(Integer)

    def __repr__(self):
        return "<Sailor(id=%s, name='%s', rating=%s)>" % (self.sid, self.sname, self.age)

class Boat(Base):
    __tablename__ = 'boats'

    bid = Column(Integer, primary_key=True)
    bname = Column(String)
    color = Column(String)
    length = Column(Integer)

    reservations = relationship('Reservation',
                                backref=backref('boat', cascade='delete'))

    def __repr__(self):
        return "<Boat(id=%s, name='%s', color=%s)>" % (self.bid, self.bname, self.color)

class Reservation(Base):
    __tablename__ = 'reserves'
    __table_args__ = (PrimaryKeyConstraint('sid', 'bid', 'day'), {})

    sid = Column(Integer, ForeignKey('sailors.sid'))
    bid = Column(Integer, ForeignKey('boats.bid'))
    day = Column(DateTime)

    sailor = relationship('Sailor')

    def __repr__(self):
        return "<Reservation(sid=%s, bid=%s, day=%s)>" % (self.sid, self.bid, self.day)

engine = create_engine( "mysql+pymysql://ari:@localhost/sailors?host=localhost")
Base.metadata.create_all(bind=engine)

Session = sessionmaker(bind = engine)

session = Session()
def test_1(): 
	q = session.query(func.count(Boat.bid), Boat.bid, Boat.bname).join(Reservation).group_by(Boat.bid).all()
	q2 = engine.execute("select count(b.bid), b.bid, b.bname from boats as b, reserves as r where r.bid = b.bid group by bid").fetchall() 
	assert q == q2

def test_2():
	sub1 = session.query(Reservation.sid, func.count(Boat.bid.distinct()).label('r_boats')).join(Reservation).filter(Boat.color == 'red').group_by(Reservation.sid).subquery()
	sub2 = session.query(func.count(Boat.color)).filter(Boat.color == "red").scalar_subquery()
	q = session.query(Sailor.sname, Sailor.sid).join(sub1).filter(sub1.c.r_boats == sub2).all() 
	real_q = engine.execute('select s.sname, s.sid from sailors as s, \
		(select r.sid, count(distinct b.bid) as r_boats from reserves as r, boats as b where b.bid = r.bid and color = "red" group by r.sid) \
			as t where t.sid = s.sid and t.r_boats = (select count(*) from boats as b where b.color = "red")').fetchall() 
	assert q == real_q

def test_3(): 
	sub1 = session.query(Sailor.sid).join(Reservation).join(Boat).filter(Boat.color == "red").subquery()
	sub2 = session.query(Reservation.sid).join(Boat).filter(Boat.color != "red")
	q = session.query(Sailor.sname.distinct(), Sailor.sid).join(sub1, sub1.c.sid == Sailor.sid).filter(sub1.c.sid.not_in(sub2)).all()
	real_q = engine.execute('select distinct s.sname, s.sid from sailors as s, \
		(select s.sid from sailors as s, reserves as r, boats as b where s.sid = r.sid and b.bid = r.bid and b.color = "red") as red_sailors\
		where red_sailors.sid = s.sid and red_sailors.sid not in \
		(select r.sid from reserves as r, boats as b where b.bid = r.bid and b.color != "red")').fetchall() 
	assert q == real_q

def test_4(): 
	sub1 = session.query(Reservation.bid, func.count(Reservation.bid).label("count_tot")).group_by(Reservation.bid).subquery()
	q = session.query(func.max(sub1.c.count_tot), sub1.c.bid).group_by(sub1.c.bid).order_by(desc(sub1.c.count_tot)).limit(1).all()
	real_q = engine.execute('select max(r.count_tot), r.bid from \
		(select s.bid, count(s.bid) as count_tot from reserves as s group by s.bid) as r group by r.bid order by r.count_tot desc limit 1').fetchall() 
	assert q == real_q

def test_5(): 
	sub1 = session.query(Reservation.sid).join(Boat).filter(Boat.color == "red")
	q = session.query(Sailor.sid, Sailor.sname).filter(Sailor.sid.not_in(sub1)).all()
	real_q = engine.execute('select s.sid, s.sname from sailors as s where s.sid not in (select r.sid from reserves as r, boats as b where b.bid = r.bid and b.color = "red")').fetchall() 
	assert q == real_q

def test_6(): 
	q = session.query(func.avg(Sailor.age)).filter(Sailor.rating == 10).all()
	real_q = engine.execute('select avg(age) from sailors where rating = 10').fetchall() 
	assert q == real_q

def test_7(): 
	sub1 = session.query(Sailor.rating, func.min(Sailor.age).label("minAge")).group_by(Sailor.rating).subquery()
	q = session.query(Sailor.sname, Sailor.sid).filter(and_(Sailor.age == sub1.c.minAge, Sailor.rating == sub1.c.rating)).all()
	real_q = engine.execute('select sailors.sname, sailors.sid from sailors,\
		(select rating, min(age) as minAge from sailors as s group by rating) sail where sailors.rating = sail.rating and sailors.age = sail.minAge').fetchall() 
	assert q == real_q

def test_8(): 
	sub1 = session.query(Reservation.sid, Reservation.bid, func.count(Reservation.bid).label('count')).group_by(Reservation.sid, Reservation.bid).subquery()
	sub2 = session.query(sub1.c.bid, func.max(sub1.c.count).label("max_c")).group_by(sub1.c.bid).subquery()
	q = session.query(sub1.c.sid, Sailor.sname, sub1.c.bid, sub2.c.max_c).filter(and_(sub1.c.bid == sub2.c.bid, sub1.c.count == sub2.c.max_c, Sailor.sid == sub1.c.sid)).all()
	real_q = engine.execute('select res.sid, s.sname, res.bid, in_res.max_c from sailors s, \
		(select bid, max(r_table.count) as max_c\
			from (select sid, bid, count(*) as count\
					from reserves group by sid, bid) as r_table group by bid) as in_res,\
						(select sid, bid, count(*) as count from reserves group by sid, bid) as res\
							where res.bid = in_res.bid and res.count = in_res.max_c and s.sid = res.sid').fetchall() 
	assert q == real_q

session.close()


