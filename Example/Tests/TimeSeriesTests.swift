//  Copyright © 2016 Quantac. All rights reserved.

import XCTest
@testable import Tally

import Foundation
import ObjectMapper
import Pantry
import SwiftDate

// TODO - add tests for concurrency behavior
class PersistentConcurrentTimeSeriesTests: XCTestCase {

    private static let TestUserDefaultKey = "PersistentConcurrentTimeSeriesTests_TestUserDefaultKey"
    
    private var timeSeries = PersistantConcurrentTimeSeries<BaseTallyDatabaseItem>(label: TestUserDefaultKey)
    
    private func tearDownRefreshTimeSeries() {
        timeSeries = PersistantConcurrentTimeSeries<BaseTallyDatabaseItem>(label: PersistentConcurrentTimeSeriesTests.TestUserDefaultKey)
    }
    
    private func assertNoError(_ error: Error?) {
        XCTAssertNil(error, ("\(error!)"))
    }
    
    override func setUp() {
        super.setUp()
        
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        // Clear anything that may be already in time series
        timeSeries.drop(until: Date() + 1.day) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(0, timeSeries.count)
    }
    
    override func tearDown() {
        super.tearDown()
    }
    
    func testEmpty() {
        XCTAssertEqual(0, timeSeries.count)
        XCTAssertNil(timeSeries.first)
        XCTAssertNil(timeSeries.last)
        XCTAssertEqual(0, timeSeries.get().count)
    }
    
    func testAppend() {
        
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item = BaseTallyDatabaseItem(timestamp: Date())
        
        timeSeries.append(item) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(1, timeSeries.count)
        XCTAssertEqual(item.timestamp, timeSeries.first!.timestamp)
        XCTAssertEqual(item.timestamp, timeSeries.last!.timestamp)
        XCTAssertEqual(1, timeSeries.get().count)
        XCTAssertEqual(item.timestamp, timeSeries.get().first!.timestamp)
    }
    
    func testAppendOutOfOrderTimeseries() {
        
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item = BaseTallyDatabaseItem(timestamp: Date())
        let failItemAppend = BaseTallyDatabaseItem(timestamp: item.timestamp - 1.second)
        
        timeSeries.append(item, completion: assertNoError)
        timeSeries.append(failItemAppend) { error in
            XCTAssertEqual(error as! TimeSeriesError, TimeSeriesError.nonAscendingOrder)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(1, timeSeries.count)
        XCTAssertEqual(item.timestamp, timeSeries.first!.timestamp)
        XCTAssertEqual(item.timestamp, timeSeries.last!.timestamp)
        XCTAssertEqual(1, timeSeries.get().count)
        XCTAssertEqual(item.timestamp, timeSeries.get().first!.timestamp)
    }
    
    func testAppendContentsOf() {
        
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item = BaseTallyDatabaseItem(timestamp: Date())
        let itemAppend = BaseTallyDatabaseItem(timestamp: Date())
        
        timeSeries.append(item, completion: assertNoError)
        timeSeries.append(contentsOf: [itemAppend]) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error)"))
        }
        
        XCTAssertEqual(2, timeSeries.count)
        XCTAssertEqual(item.timestamp, timeSeries.first!.timestamp)
        XCTAssertEqual(itemAppend.timestamp, timeSeries.last!.timestamp)
    }
    
    func testAppendContentsOfEmptyTimeseries() {
        
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let itemAppend = BaseTallyDatabaseItem(timestamp: Date())

        timeSeries.append(contentsOf: [itemAppend]) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(1, timeSeries.count)
        XCTAssertEqual(itemAppend.timestamp, timeSeries.first!.timestamp)
    }

    func testAppendContentsOfEmptyList() {
        
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item = BaseTallyDatabaseItem(timestamp: Date())
        
        timeSeries.append(item, completion: assertNoError)
        timeSeries.append(contentsOf: []) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }

        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }

        XCTAssertEqual(1, timeSeries.count)
    }
    
    func testAppendContentsOfOutOfOrderArgument() {
        
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item = BaseTallyDatabaseItem(timestamp: Date())
        let item2 = BaseTallyDatabaseItem(timestamp: item.timestamp - 1.second)
        
        timeSeries.append(contentsOf: [item, item2]) { error in
            XCTAssertEqual(error as! TimeSeriesError, TimeSeriesError.nonAscendingOrderArgument)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(0, timeSeries.count)
    }
    
    func testAppendContentsOfOutOfOrderTimeseries() {
        
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item = BaseTallyDatabaseItem(timestamp: Date())
        let failItemAppend = BaseTallyDatabaseItem(timestamp: item.timestamp - 1.second)
        
        timeSeries.append(item, completion: assertNoError)
        timeSeries.append(contentsOf: [failItemAppend]) { error in
            XCTAssertEqual(error as! TimeSeriesError, TimeSeriesError.nonAscendingOrder)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(1, timeSeries.count)
        XCTAssertEqual(item.timestamp, timeSeries.last!.timestamp)
    }
    
    func testPrepend() {
        
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let itemPrepend = BaseTallyDatabaseItem(timestamp: Date())
        let item = BaseTallyDatabaseItem(timestamp: Date())
        
        timeSeries.append(item, completion: assertNoError)
        timeSeries.prepend(contentsOf: [itemPrepend]) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error)"))
        }
        
        XCTAssertEqual(2, timeSeries.count)
        XCTAssertEqual(item.timestamp, timeSeries.last!.timestamp)
        XCTAssertEqual(itemPrepend.timestamp, timeSeries.first!.timestamp)
    }
    
    func testPrependEmptyTimeseries() {
        
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let itemPrepend = BaseTallyDatabaseItem(timestamp: Date())
        
        timeSeries.prepend(contentsOf: [itemPrepend]) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(1, timeSeries.count)
        XCTAssertEqual(itemPrepend.timestamp, timeSeries.first!.timestamp)
    }
    
    func testPrependEmptyList() {
        
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item = BaseTallyDatabaseItem(timestamp: Date())
        
        timeSeries.append(item, completion: assertNoError)
        timeSeries.prepend(contentsOf: []) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(1, timeSeries.count)
    }
    
    func testPrependOutOfOrderArgument() {
        
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item = BaseTallyDatabaseItem(timestamp: Date())
        let item2 = BaseTallyDatabaseItem(timestamp: item.timestamp - 1.second)
        
        timeSeries.prepend(contentsOf: [item, item2]) { error in
            XCTAssertEqual(error as! TimeSeriesError, TimeSeriesError.nonAscendingOrderArgument)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(0, timeSeries.count)
    }
    
    func testPrependOutOfOrderTimeseries() {
        
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item = BaseTallyDatabaseItem(timestamp: Date())
        let failItemPrepend = BaseTallyDatabaseItem(timestamp: item.timestamp + 1.second)
        
        timeSeries.append(item, completion: assertNoError)
        timeSeries.prepend(contentsOf: [failItemPrepend]) { error in
            XCTAssertEqual(error as! TimeSeriesError, TimeSeriesError.nonAscendingOrder)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(1, timeSeries.count)
        XCTAssertEqual(item.timestamp, timeSeries.last!.timestamp)
    }
    
    func testDrop() {

        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item = BaseTallyDatabaseItem(timestamp: Date())
        
        timeSeries.append(item, completion: assertNoError)
        timeSeries.drop(until: item.timestamp + 1.seconds) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(0, timeSeries.count)
    }
    
    func testGetSince() {
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item = BaseTallyDatabaseItem(timestamp: Date())
        
        let getBeforeTimestamp = item.timestamp - 1.seconds
        let getAfterTimestamp = item.timestamp + 1.seconds

        timeSeries.append(item) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(1, timeSeries.get(since: getBeforeTimestamp).count)
        XCTAssertEqual(0, timeSeries.get(since: getAfterTimestamp).count)
    }
    
    func testGetSinceNonInclusiveBehavior() {
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item0 = BaseTallyDatabaseItem(timestamp: Date())
        let item1 = BaseTallyDatabaseItem(timestamp: item0.timestamp + 1.second)
        let item2 = BaseTallyDatabaseItem(timestamp: item1.timestamp + 1.second)
        let item3 = BaseTallyDatabaseItem(timestamp: item2.timestamp + 1.second)
        let item4 = BaseTallyDatabaseItem(timestamp: item3.timestamp + 1.second)
        
        timeSeries.append(item0, completion: assertNoError)
        timeSeries.append(item1, completion: assertNoError)
        timeSeries.append(item2, completion: assertNoError)
        timeSeries.append(item3, completion: assertNoError)
        timeSeries.append(item4) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }

        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        let slice = timeSeries.get(since: item2.timestamp)
        XCTAssertEqual(2, slice.count)
        XCTAssertEqual(item3.timestamp, slice.first!.timestamp)
        XCTAssertEqual(item4.timestamp, slice.last!.timestamp)
    }
    
    func testDropTimestampBeforeAppend() {

        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item = BaseTallyDatabaseItem(timestamp: Date())
        let dropBeforeTimestamp = item.timestamp - 1.seconds

        
        timeSeries.append(item, completion: assertNoError)
        timeSeries.drop(until: dropBeforeTimestamp) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(1, timeSeries.count)
    }

    func testDropUntilInclusiveBehavior() {
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item0 = BaseTallyDatabaseItem(timestamp: Date())
        let item1 = BaseTallyDatabaseItem(timestamp: item0.timestamp + 1.second)
        let item2 = BaseTallyDatabaseItem(timestamp: item1.timestamp + 1.second)
        let item3 = BaseTallyDatabaseItem(timestamp: item2.timestamp + 1.second)
        let item4 = BaseTallyDatabaseItem(timestamp: item3.timestamp + 1.second)
        
        timeSeries.append(item0, completion: assertNoError)
        timeSeries.append(item1, completion: assertNoError)
        timeSeries.append(item2, completion: assertNoError)
        timeSeries.append(item3, completion: assertNoError)
        timeSeries.append(item4, completion: assertNoError)
        
        timeSeries.drop(until: item2.timestamp) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(2, timeSeries.count)
        XCTAssertEqual(item3.timestamp, timeSeries.first!.timestamp)
        XCTAssertEqual(item4.timestamp, timeSeries.last!.timestamp)
    }
    
    func testAppendDropAppend() {
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item = BaseTallyDatabaseItem(timestamp: Date())
        
        timeSeries.append(item, completion: assertNoError)
        timeSeries.drop(until: Date(), completion: assertNoError)
        timeSeries.append(item) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(1, timeSeries.count)
    }
    
    func testTearDownRefresh() {
        
        let asyncExpectation = expectation(description:  "AWS callbacks")
        
        let item = BaseTallyDatabaseItem(timestamp: Date())
        
        timeSeries.append(item) { error in
            self.assertNoError(error)
            asyncExpectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 20) { error in
            XCTAssertNil(error, ("Test timeout: \(error!)"))
        }
        
        XCTAssertEqual(1, timeSeries.count)
        
        tearDownRefreshTimeSeries()
        
        XCTAssertEqual(1, timeSeries.count)
        XCTAssertEqual(item.timestamp, timeSeries.first!.timestamp)
    }
    
    func testTearDownRefreshEmpty() {
        XCTAssertEqual(0, timeSeries.count)
        
        tearDownRefreshTimeSeries()
        
        XCTAssertEqual(0, timeSeries.count)
    }
}
