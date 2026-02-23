import os
import time
from contextlib import asynccontextmanager

import mysql.connector
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel


class UserCreate(BaseModel):
    name: str
    email: str


class PostCreate(BaseModel):
    user_id: int
    title: str
    body: str


def get_db():
    conn = mysql.connector.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
    )
    try:
        yield conn
    finally:
        conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    for _ in range(30):
        try:
            conn = mysql.connector.connect(
                host=os.environ["DB_HOST"],
                user=os.environ["DB_USER"],
                password=os.environ["DB_PASSWORD"],
                database=os.environ["DB_NAME"],
            )
            cursor = conn.cursor()
            with open("init.sql") as f:
                for statement in f.read().split(";"):
                    statement = statement.strip()
                    if statement:
                        cursor.execute(statement)
            conn.commit()
            cursor.close()
            conn.close()
            break
        except mysql.connector.Error:
            time.sleep(1)
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/users")
def create_user(user: UserCreate, conn=Depends(get_db)):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO users (name, email) VALUES (%s, %s)", (user.name, user.email))
    conn.commit()
    user_id = cursor.lastrowid
    cursor.close()
    return {"id": user_id, "name": user.name, "email": user.email}


@app.get("/users")
def list_users(conn=Depends(get_db)):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users")
    users = cursor.fetchall()
    cursor.close()
    return users


@app.get("/users/{user_id}")
def get_user(user_id: int, conn=Depends(get_db)):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
    user = cursor.fetchone()
    cursor.close()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@app.put("/users/{user_id}")
def update_user(user_id: int, user: UserCreate, conn=Depends(get_db)):
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET name = %s, email = %s WHERE id = %s", (user.name, user.email, user_id))
    conn.commit()
    cursor.close()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="User not found")
    return {"id": user_id, "name": user.name, "email": user.email}


@app.delete("/users/{user_id}")
def delete_user(user_id: int, conn=Depends(get_db)):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
    conn.commit()
    cursor.close()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="User not found")
    return {"detail": "User deleted"}


@app.post("/posts")
def create_post(post: PostCreate, conn=Depends(get_db)):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO posts (user_id, title, body) VALUES (%s, %s, %s)", (post.user_id, post.title, post.body))
    conn.commit()
    post_id = cursor.lastrowid
    cursor.close()
    return {"id": post_id, "user_id": post.user_id, "title": post.title, "body": post.body}


@app.get("/posts")
def list_posts(conn=Depends(get_db)):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM posts")
    posts = cursor.fetchall()
    cursor.close()
    return posts


@app.get("/posts/{post_id}")
def get_post(post_id: int, conn=Depends(get_db)):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM posts WHERE id = %s", (post_id,))
    post = cursor.fetchone()
    cursor.close()
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post


@app.put("/posts/{post_id}")
def update_post(post_id: int, post: PostCreate, conn=Depends(get_db)):
    cursor = conn.cursor()
    cursor.execute("UPDATE posts SET user_id = %s, title = %s, body = %s WHERE id = %s", (post.user_id, post.title, post.body, post_id))
    conn.commit()
    cursor.close()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Post not found")
    return {"id": post_id, "user_id": post.user_id, "title": post.title, "body": post.body}


@app.delete("/posts/{post_id}")
def delete_post(post_id: int, conn=Depends(get_db)):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM posts WHERE id = %s", (post_id,))
    conn.commit()
    cursor.close()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Post not found")
    return {"detail": "Post deleted"}


@app.get("/users/{user_id}/posts")
def get_user_posts(user_id: int, conn=Depends(get_db)):
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT posts.id, posts.title, posts.body, users.name, users.email "
        "FROM posts JOIN users ON posts.user_id = users.id "
        "WHERE users.id = %s",
        (user_id,),
    )
    posts = cursor.fetchall()
    if not posts:
        cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        cursor.close()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return {"user": user, "posts": []}
    cursor.close()
    user = {"id": user_id, "name": posts[0]["name"], "email": posts[0]["email"]}
    return {
        "user": user,
        "posts": [{"id": p["id"], "title": p["title"], "body": p["body"]} for p in posts],
    }


# TODO: Add Pydantic model for CommentCreate
# TODO: Add CRUD endpoints for comments
# TODO: Add GET /posts/{post_id}/comments endpoint
#       Follow the pattern in get_user_posts above:
#       use a JOIN between posts and comments to return
#       {"post": {post fields}, "comments": [{comment fields}, ...]}
