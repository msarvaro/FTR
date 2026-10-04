"""Timetable service: generates a weekly timetable for a course (no db, pure compute)"""
import hashlib

from common import INSTANCE, make_app

app = make_app("timetable-service")
DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri"]
ROOMS = ["C1.1.239", "C1.2.244", "C1.3.361", "C2.1.101", "Online"]


@app.get("/timetable/{course}")
def timetable(course: str):
    h = int(hashlib.sha256(course.encode()).hexdigest(), 16)
    slots = []
    for i in range(3):
        slots.append({"day": DAYS[(h >> (i * 4)) % 5], "start": f"{9 + (h >> (i * 7)) % 8}:00",
                      "room": ROOMS[(h >> (i * 3)) % 5], "type": ["lecture", "practice", "lab"][i]})
    return {"course": course, "slots": slots, "served_by": INSTANCE}
