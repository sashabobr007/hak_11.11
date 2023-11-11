from fastapi import FastAPI, Depends, File, UploadFile, Response
import uvicorn
from fastapi.responses import FileResponse
from starlette.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import os
from schemas import *
import bcrypt
from database import get_db
from models import *
import csv
from algos import *


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def check_input_dot(task: Task):
    if not isinstance(task.adres, str):
        return 'Нужно ввести действительный адрес'
    if task.when_created not in ('вчера', 'давно'):
        return 'Нужно ввести значение "вчера" или "давно"'
    if task.all_cards_delivered not in ('да', 'нет'):
        return 'Нужно ввести значение "да" или "нет"'
    if not (isinstance(task.days_after, int) and isinstance(task.quantity_accepted_tasks, int) and isinstance(task.quantity_cards, int)):
        return 'Нужно ввести число'
    return 'succeess'


@app.get("/getimage/")
async def im_get(fio : str):
    names = os.listdir(os.getcwd())
    filename = f"results.jpg"
    for name in names:
        if name.startswith('Боб'):
            filename = 'Боб.png'
        else:
            if name.startswith(fio):
                filename = name
    return FileResponse(filename)


@app.get("/timesheet/")
def get_timesheet(db: Session = Depends(get_db)):
    result = []
    workers = db.query(WorkersTask).all()
    for worker in workers:
        tasks = db.query(TimesheetTask).filter_by(fio=worker.fio).all()
        row = {
            "worker": worker,
            "tasks" : tasks
        }
        result.append(row)

    return result


@app.get("/")
async def root():
    return {"hello": "success"}


@app.post("/to_default/")
async def to_default():
    os.remove('dots.csv')
    dots = pd.read_csv('dots_default.csv', sep=';', index_col=0)
    dots.to_csv('dots.csv', sep=';')
    algoritm()
    to_bd_day_tasks()
    to_bd_timesheet()
    to_bd_workers()
    return {"message": "success"}


@app.put("/change_status/")
async def change_status(id : int, status :int, db: Session = Depends(get_db)):
    try:
        dot = db.query(TimesheetTask).filter_by(id=id).first()
        dot.status = status
        db.commit()
        return {"message": "success"}
    except:
        return {"message": "error"}


@app.get("/analitika/")
async def analitika(db: Session = Depends(get_db)):
    res = analitica()
    to_bd_analitika()
    result = {
        "analitika_admin": res,
        "worker_analitika" : db.query(WorkerAn).all()
    }

    return result


@app.get("/exel/")
async def exel():
    filename = "Dataset.xlsx"
    return FileResponse(filename, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        filename=filename)

@app.post("/exel/")
async def exel_append(file: UploadFile = File(...)):
    try:
        with open(file.filename, "wb") as buffer:
            buffer.write(file.file.read())
        if append_from_exel(file.filename) == 'success':
            #os.remove(file.filename)
            algoritm()
            to_bd_day_tasks()
            to_bd_timesheet()
            to_bd_workers()
            return {"message": "success"}

        else:
            os.remove(file.filename)

            return {"message": "error"}
    except:
        os.remove(file.filename)

        return {"message": "error"}



@app.get("/get_points/")
async def point(db: Session = Depends(get_db)):
     return db.query(Point).all()


@app.put("/task/")
async def task(task : Task):
    try:
        res = check_input_dot(task)
        if  res == 'succeess':
            with open('dots.csv', 'a', newline='') as file_write:
                writer = csv.writer(file_write, delimiter=';')
                writer.writerow([task.id_point, task.adres, task.when_created, task.all_cards_delivered, task.days_after, task.quantity_accepted_tasks, task.quantity_cards])
            algoritm()
            to_bd_day_tasks()
            to_bd_timesheet()
            to_bd_workers()
            return {"message": "success"}
        else:
            return {"message": res}

    except:
        return {"message": "error"}


@app.post("/user_auth/")
async def user_auth(user : User, db: Session = Depends(get_db)):
    if user.fio == 'admin' and user.password == '1111':
        return {"auth": "admin"}
    else:
        try:
            db_pass = db.query(Worker).filter_by(fio=user.fio).first().password
            password_encode = user.password.encode()
            by = bytes(db_pass.encode())
            if bcrypt.checkpw(password_encode, by):
                return {"auth" : "success"}
            else:
                return {"auth": "password"}

        except:
            return {"auth": "login"}


if __name__ == '__main__':
    uvicorn.run(app, port=8000)