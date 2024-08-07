// The Swift Programming Language
// https://docs.swift.org/swift-book
import Foundation
import SQLite3

let SQLITE_DATE = SQLITE_NULL + 1
private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

public enum DBError: Error {
  case failedToConnect
  case databaseClosed
  case failedToPrepare
  case failedToExecute
}

public enum Arg {
  case string(String)
  case int(any BinaryInteger)
  case float(any BinaryFloatingPoint)
  case blob(Data)
  case null
}

public final class Database {
  public static let shared = Database()
  
  var db: OpaquePointer?
  
  private init() {
    self.db = nil
  }
  
  public func connect(path: String) throws {
    let result = sqlite3_open(path, &self.db)
    
    guard result == SQLITE_OK else {
      print("Unable to open database. Error code: \(result)")
      throw DBError.failedToConnect
    }
    
    print("Successfully opened connection to database at \(path)")
  }
  
  deinit {
    sqlite3_close(self.db)
  }
  
  private func prepare(sql: String, args: [Arg]) throws -> OpaquePointer? {
    var stmt: OpaquePointer?
    let cstring = sql.cString(using: .utf8)
    
    guard sqlite3_prepare_v2(self.db, cstring, -1, &stmt, nil) == SQLITE_OK else {
      sqlite3_finalize(stmt)
      let error = String(cString: sqlite3_errmsg(self.db))
      NSLog("Error: \(error)")
      
      throw DBError.failedToPrepare
    }
    
    if args.count == 0 {
      return stmt
    }
    
    let argRefs = sqlite3_bind_parameter_count(stmt)
    if argRefs != args.count {
      NSLog("Mismatch in number of arguments: \(argRefs) ?'s vs \(args.count) args")
      throw DBError.failedToPrepare
    }
    
    var flag: CInt = 0
    for (i, arg) in args.enumerated() {
      let bindIndex = Int32(i + 1)
      switch arg {
      case .string(let value):
        flag = sqlite3_bind_text(stmt, bindIndex, NSString(string: value).utf8String, -1, SQLITE_TRANSIENT)
      case .int(let value):
        flag = sqlite3_bind_int(stmt, bindIndex, CInt(value))
      case .float(let value):
        flag = sqlite3_bind_double(stmt, bindIndex, CDouble(value))
      case .blob(let value):
        let nsdata = NSData(data: value)
        flag = sqlite3_bind_blob(stmt, bindIndex, nsdata.bytes, CInt(nsdata.length), SQLITE_TRANSIENT)
      case .null:
        flag = sqlite3_bind_null(stmt, bindIndex)
      }
      
      if flag != SQLITE_OK {
        sqlite3_finalize(stmt)
        NSLog("Error binding: \(arg),\nflat: \(flag)")
        throw DBError.failedToPrepare
      }
    }
    
    return stmt
  }
  
  private func execute(stmt: OpaquePointer, sql: String) throws -> Int? {
    let res = sqlite3_step(stmt)
    if res != SQLITE_DONE && res != SQLITE_OK {
      sqlite3_finalize(stmt)
      let error = String(cString: sqlite3_errmsg(self.db))
      NSLog("\nFailed to execute:\n\(sql)\nResponse: \(res)\nError: \(error)")
      throw DBError.failedToExecute
    }
    
    let upp = sql.uppercased()
    var result = 0
    if upp.hasPrefix("INSERT ") {
      // Known limitations: http://www.sqlite.org/c3ref/last_insert_rowid.html
      let rid = sqlite3_last_insert_rowid(db)
      result = Int(rid)
    } else if upp.hasPrefix("DELETE") || upp.hasPrefix("UPDATE") {
      var cnt = sqlite3_changes(db)
      if cnt == 0 {
        cnt += 1
      }
      result = Int(cnt)
    } else {
      result = 1
    }
    // Finalize
    sqlite3_finalize(stmt)
    return result
  }
  
  private func query(stmt: OpaquePointer, sql: String) -> [[String: Any]]? {
    var rows = [[String: Any]]()
    var fetchColumnInfo = true
    var columnCount: CInt = 0
    var columnNames = [String]()
    var columnTypes = [CInt]()
    var result = sqlite3_step(stmt)
    while result == SQLITE_ROW {
      // Only get the first row's column info
      if fetchColumnInfo {
        columnCount = sqlite3_column_count(stmt)
        for index in 0..<sqlite3_column_count(stmt) {
          // Get column name
          let name = sqlite3_column_name(stmt, index)
          columnNames.append(String(validatingUTF8: name!)!)
          // Get column type
          columnTypes.append(getColumnType(index: index, stmt: stmt))
        }
        fetchColumnInfo = false
      }
      // Get row data for each column
      var row = [String: Any]()
      for index in 0..<columnCount {
        let key = columnNames[Int(index)]
        let type = columnTypes[Int(index)]
        if let val = getColumnValue(index: index, type: type, stmt: stmt) {
          row[key] = val
        }
      }
      rows.append(row)
      // Next row
      result = sqlite3_step(stmt)
    }
    sqlite3_finalize(stmt)
    return rows
  }
  
  private func getColumnType(index: CInt, stmt: OpaquePointer) -> CInt {
    var type: CInt = 0
    // Column types - http://www.sqlite.org/datatype3.html (section 2.2 table column 1)
    let blobTypes = ["BINARY", "BLOB", "VARBINARY"]
    let charTypes = ["CHAR", "CHARACTER", "CLOB", "NATIONAL VARYING CHARACTER", "NATIVE CHARACTER", "NCHAR", "NVARCHAR", "TEXT", "VARCHAR", "VARIANT", "VARYING CHARACTER"]
    let dateTypes = ["DATE", "DATETIME", "TIME", "TIMESTAMP"]
    let intTypes = ["BIGINT", "BIT", "BOOL", "BOOLEAN", "INT", "INT2", "INT8", "INTEGER", "MEDIUMINT", "SMALLINT", "TINYINT"]
    let nullTypes = ["NULL"]
    let realTypes = ["DECIMAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT", "NUMERIC", "REAL"]
    // Determine type of column - http://www.sqlite.org/c3ref/c_blob.html
    let buf = sqlite3_column_decltype(stmt, index)
    if buf != nil {
      var tmp = String(validatingUTF8: buf!)!.uppercased()
      // Remove bracketed section
      if let pos = tmp.range(of: "(") {
        tmp = String(tmp[..<pos.lowerBound])
      }
      // Remove unsigned?
      // Remove spaces
      // Is the data type in any of the pre-set values?
      if intTypes.contains(tmp) {
        return SQLITE_INTEGER
      }
      if realTypes.contains(tmp) {
        return SQLITE_FLOAT
      }
      if charTypes.contains(tmp) {
        return SQLITE_TEXT
      }
      if blobTypes.contains(tmp) {
        return SQLITE_BLOB
      }
      if nullTypes.contains(tmp) {
        return SQLITE_NULL
      }
      if dateTypes.contains(tmp) {
        return SQLITE_DATE
      }
      return SQLITE_TEXT
    } else {
      // For expressions and sub-queries
      type = sqlite3_column_type(stmt, index)
    }
    return type
  }
  
  private func getColumnValue(index: CInt, type: CInt, stmt: OpaquePointer) -> Any? {
    // Integer
    if type == SQLITE_INTEGER {
      let val = sqlite3_column_int64(stmt, index)
      return Int(val)
    }
    // Float
    if type == SQLITE_FLOAT {
      let val = sqlite3_column_double(stmt, index)
      return Double(val)
    }
    // Blob
    if type == SQLITE_BLOB {
      let data = sqlite3_column_blob(stmt, index)
      let size = sqlite3_column_bytes(stmt, index)
      let val = NSData(bytes: data, length: Int(size))
      return Data(val).base64EncodedString()
    }
    // Null
    if type == SQLITE_NULL {
      return nil
    }
    // Text - handled by default handler at end
    // If nothing works, return a string representation
    if let ptr = UnsafeRawPointer(sqlite3_column_text(stmt, index)) {
      let uptr = ptr.bindMemory(to: CChar.self, capacity: 0)
      let txt = String(validatingUTF8: uptr)
      return txt
    }
    return nil
  }
  
  public func execute(_ sql: String, _ args: [Arg] = []) throws -> Int {
    guard self.db != nil else {
      throw DBError.databaseClosed
    }
    
    guard let stmt = try self.prepare(sql: sql, args: args) else {
      throw DBError.failedToPrepare
    }
    
    guard let result = try self.execute(stmt: stmt, sql: sql) else {
      throw DBError.failedToExecute
    }
    
    return result
  }
  
  public func query(_ sql: String, _ args: [Arg] = []) throws -> [[String: Any]] {
    guard self.db != nil else {
      throw DBError.databaseClosed
    }
    
    guard let stmt = try self.prepare(sql: sql, args: args) else {
      throw DBError.failedToPrepare
    }
    
    guard let result = self.query(stmt: stmt, sql: sql) else {
      throw DBError.failedToExecute
    }
    
    return result
  }
}

// QUERY =======================================================================

public protocol Queryable: Codable {
  // column name to field name
  static var cols: [String: String] { get }
}

extension Queryable {
  static var cols: [String: String] { [:] }
  
  public static func query(_ sql: String, _ args: [Arg] = []) throws -> [Self] {
    let rows = try Database.shared.query(sql, args)
    return try Self.fromRows(rows)
  }
  
  public static func fromRows(_ rows: [[String: Any]]) throws -> [Self] {
    return try rows.map { row in
      var row = row
      for key in Self.cols.keys {
        row.switchKey(from: key, to: Self.cols[key]!)
      }
      let data = try JSONSerialization.data(withJSONObject: row)
      return try JSONDecoder().decode(Self.self, from: data)
    }
  }
}

// INSERT ======================================================================

public protocol Insertable {
  // table name
  static var table: String { get }
  // column name to args
  var inserts: [String: Arg] { get }
}

extension Insertable {
  fileprivate func createSql() -> (String, [Arg]) {
    let keys = Array(self.inserts.keys)
    let sql =
      """
        INSERT INTO \(Self.table)
          (\(keys.joined(separator: ", ")))
        VALUES
          (\(self.inserts.values.map { _ in "?" }.joined(separator: ", ")))
      """
    let args = keys.map { self.inserts[$0]! }
    return (sql, args)
  }
}

extension Array where Element: Insertable {
  // all elements should be of the same type and the array should not be empty
  public func insert() -> (String, [Arg]) {
    let keys = self.first!.inserts.keys
    let sql =
      """
        INSERT INTO \(Self.Element.table)
          (\(keys.joined(separator: ", ")))
        VALUES
          \(self.map({ "(" + $0.inserts.keys.map({ _ in "?" }).joined(separator: ", ")  + ")" }).joined(separator: ", "))
      """
    let args = self.flatMap({ obj in keys.map({ obj.inserts[$0]! }) })

    return (sql, args)
  }
}

extension Database {
  public func insert<T: Insertable>(_ object: T) throws -> Int {
    let (sql, args) = object.createSql()
    return try Database.shared.execute(sql, args)
  }
  
  public func insert<T: Insertable>(_ objects: [T]) throws -> Int {
    if objects.isEmpty { return 0 }
    let (sql, args) = objects.insert()
    return try Database.shared.execute(sql, args)
  }
}

// TRANSACTION =================================================================

enum StatementType {
  case execute
  case query
}

struct Statement {
  let type: StatementType
  let sql: String
  let args: [Arg]
}

public final class Transaction {
  var stmts: [Statement]
  
  init() {
    self.stmts = []
  }
  
  public func execute(_ sql: String, _ args: [Arg] = []) -> Self {
    self.stmts.append(Statement(type: .execute, sql: sql, args: args))
    return self
  }
  
  public func query(_ sql: String, _ args: [Arg] = []) -> Self {
    self.stmts.append(Statement(type: .query, sql: sql, args: args))
    return self
  }
  
  public func insert<T: Insertable>(_ object: T) -> Self {
    let (sql, args) = object.createSql()
    self.stmts.append(Statement(type: .execute, sql: sql, args: args))
    return self
  }
  
  public func insert<T: Insertable>(_ objects: [T]) -> Self {
    if objects.isEmpty { return self }
    let (sql, args) = objects.insert()
    self.stmts.append(Statement(type: .execute, sql: sql, args: args))
    return self
  }
  
  public func run() throws -> [Any] {
    do {
      let _ = try Database.shared.beginTransaction()
      
      var results = [Any]()
      for stmt in self.stmts {
        switch stmt.type {
        case .execute:
          let res = try Database.shared.execute(stmt.sql, stmt.args)
          results.append(res)
        case .query:
          let res = try Database.shared.query(stmt.sql, stmt.args)
          results.append(res)
        }
      }
      
      let _ = try Database.shared.commit()
      
      return results
    } catch {
      let _ = try Database.shared.rollback()
      throw error
    }
  }
}

extension Database {
  public func beginTransaction() throws -> Int {
    return try Database.shared.execute("BEGIN TRANSACTION")
  }
  
  public func commit() throws -> Int {
    return try Database.shared.execute("COMMIT")
  }
  
  public func rollback() throws -> Int {
    return try Database.shared.execute("ROLLBACK")
  }
  
  public func transaction() -> Transaction {
    return Transaction()
  }
}

// HELPER ======================================================================
extension Dictionary<String, Any> {
  mutating func switchKey(from: String, to: String) {
    if let entry = self.removeValue(forKey: from) {
      self[to] = entry
    }
  }
}
