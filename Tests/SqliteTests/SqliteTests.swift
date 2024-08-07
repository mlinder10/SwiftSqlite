import XCTest
@testable import Sqlite

struct User: Queryable, Insertable {
  let id: Int
  let name: String
  let weight: Double
  let meta: Data
  let gf: Int?
  
  static var table: String { "users" }
  var inserts: [String: Arg] {[
    "id": .int(id),
    "name": .string(name),
    "weight": .float(weight),
    "meta": .blob(meta),
    "gf": gf == nil ? .null : .int(gf!)
  ]}
}

struct Posts: Queryable, Insertable {
  let id: Int
  let title: String
  let body: String
  let userId: Int
  
  static var table: String { "posts" }
  static var cols: [String: String] {["user_id": "userId"]}
  var inserts: [String: Arg] {[
    "id": .int(id),
    "title": .string(title),
    "body": .string(body),
    "user_id": .int(userId)
  ]}
}

final class SqliteTests: XCTestCase {
  func testExample() throws {

    let path = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first! + "/test.sqlite3"
    try Database.shared.connect(path: path)

    let _ = try Database.shared.execute("DROP TABLE IF EXISTS users")

    let _ = try Database.shared.execute("DROP TABLE IF EXISTS posts")
    
    let _ = try Database.shared.execute(
      """
        CREATE TABLE IF NOT EXISTS users (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          weight FLOAT NOT NULL,
          meta BLOB NOT NULL,
          gf INTEGER
        )
      """
    )

    let _ = try Database.shared.execute(
      """
        CREATE TABLE IF NOT EXISTS posts (
          id INTEGER PRIMARY KEY,
          title TEXT NOT NULL,
          body TEXT NOT NULL,
          user_id INTEGER NOT NULL,
          FOREIGN KEY (user_id) REFERENCES users(id)
        )
      """
    )

    let multiUser = [
      User(id: 1, name: "John", weight: 70.5, meta: Data([1, 2, 3]), gf: 2),
      User(id: 2, name: "Jane", weight: 60.5, meta: Data([4, 5, 6]), gf: nil),
    ]

    let singleUser = User(id: 3, name: "Jack", weight: 50.5, meta: Data([7, 8, 9]), gf: 3)

    let _ = try Database.shared.insert(multiUser)
    let _ = try Database.shared.insert(singleUser)

    let multiPosts = [
      Posts(id: 1, title: "Post 1", body: "Body 1", userId: 1),
      Posts(id: 2, title: "Post 2", body: "Body 2", userId: 1),
      Posts(id: 3, title: "Post 3", body: "Body 3", userId: 2),
    ]

    // let _ = try Database.shared.insert(multiPosts)
    let _ = try Database.shared.transaction()
      .insert(multiPosts)
      .run()

    let users = try User.query("SELECT * FROM users ORDER BY id ASC")

    let posts = try Posts.query("SELECT * FROM posts WHERE user_id = ?", [.int(1)])

    print(users)
    print(posts)

    let _ = try Database.shared.execute("DELETE FROM users")
    let _ = try Database.shared.execute("DROP TABLE users")
    let _ = try Database.shared.execute("DROP TABLE posts")
  }
}
