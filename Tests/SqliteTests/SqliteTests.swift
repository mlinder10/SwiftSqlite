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

final class SqliteTests: XCTestCase {
  func testExample() throws {
    // XCTest Documentation
    // https://developer.apple.com/documentation/xctest
    
    // Defining Test Cases and Test Methods
    // https://developer.apple.com/documentation/xctest/defining_test_cases_and_test_methods
    let path = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first! + "/test.sqlite3"
    try Database.shared.connect(path: path)

    let _ = try Database.shared.execute("DROP TABLE IF EXISTS users")
    
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

    let multiUser = [
      User(id: 1, name: "John", weight: 70.5, meta: Data([1, 2, 3]), gf: 2),
      User(id: 2, name: "Jane", weight: 60.5, meta: Data([4, 5, 6]), gf: nil),
    ]

    let singleUser = User(id: 3, name: "Jack", weight: 50.5, meta: Data([7, 8, 9]), gf: 3)

    let _ = try Database.shared.insert(multiUser)
    let _ = try Database.shared.insert(singleUser)

    let users = try User.query("SELECT * FROM users ORDER BY id ASC")

    print(users)

    let _ = try Database.shared.execute("DELETE FROM users")

    let _ = try Database.shared.execute("DROP TABLE users")
  }
}
