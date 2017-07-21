// Copyright © 2017 Quantac. All rights reserved.

import Foundation
import Pantry // Wraps user defaults; makes it easier to read from / persist data to file

// Threadsafe container for queueing/dequeing data that persists 
// data to disk.
//
// A more efficient implementation could be to manage various 
// buckets e.g. a smaller one that is used for quick storage
// and a large one that is periodically overwritten (takes longer 
// to read, append, rewrite as list size grows)
public class PersistantConcurrentTimeSeries<T> where T: TimeSeriesItem, T: Storable {
    
    private let label: String

    // Serialize requests to modify data
    private let dataOpsSerializationQueue : DispatchQueue
    
    // Synchronization is used for write only to ensure consistency
    // for adding/removing elements from underlying array.
    //
    // From https://developer.apple.com/swift/blog/?id=10
    // In Swift, Array, String, and Dictionary are all value types. 
    // They behave much like a simple int value in C, acting as a 
    // unique instance of that data. You don’t need to do anything special — 
    // such as making an explicit copy — to prevent other code from modifying 
    // that data behind your back. Importantly, you can safely pass copies of
    // values across threads without synchronization. In the spirit of 
    // improving safety, this model will help you write more predictable code 
    // in Swift.
    private var inMemoryData : [T]

    public var count : Int {
        get {
            return inMemoryData.count
        }
    }
    
    public var first : T? {
        get {
            return inMemoryData.first
        }
    }
    
    public var last : T? {
        get {
            return inMemoryData.last
        }
    }
    
    // "label" should be unique - key used to persist data (and
    //  for serialization queue)
    public init (label: String) {

        let label = "PersistantConcurrentQueue_\(label)"

        self.label = label

        dataOpsSerializationQueue = DispatchQueue(label: label)

        // Only time should ever need to read from persistant storage
        // is when initializing memory buffer.
        inMemoryData = Pantry.unpack(label) ?? []
    }

    // Current design assumes items will always be in ascending time order.
    public func append(_ newElement: T, completion: @escaping ((Error?) -> ())) {
        dataOpsSerializationQueue.async { [weak self] in
            if let selfReference = self {

                guard selfReference.inMemoryData.last == nil ||
                    newElement.timestamp >= selfReference.inMemoryData.last!.timestamp else {
                    completion(TimeSeriesError.nonAscendingOrder)
                    return
                }
                
                selfReference.inMemoryData.append(newElement)
                Pantry.pack(selfReference.inMemoryData, key: selfReference.label)
                completion(nil)
            }
        }
    }

    // Current design assumes items will always be in ascending time order.
    public func append(contentsOf: Array<T>, completion: @escaping ((Error?) -> ())) {
        dataOpsSerializationQueue.async { [weak self] in
            if let selfReference = self {
                
                // Not technically a failure, just exit early in empty boundary case
                guard contentsOf.count > 0 else {
                    completion(nil)
                    return
                }
                
                guard PersistantConcurrentTimeSeries.isSorted(contentsOf) else {
                    completion(TimeSeriesError.nonAscendingOrderArgument)
                    return
                }
                
                guard selfReference.inMemoryData.last == nil ||
                    contentsOf.first!.timestamp >= selfReference.inMemoryData.last!.timestamp else {
                        completion(TimeSeriesError.nonAscendingOrder)
                        return
                }
                
                selfReference.inMemoryData = selfReference.inMemoryData + contentsOf
                Pantry.pack(selfReference.inMemoryData, key: selfReference.label)
                completion(nil)
            }
                
            else {
                completion(nil)
            }
        }
    }
    
    // Current design assumes items will always be in ascending time order.
    public func prepend(contentsOf: Array<T>, completion: @escaping ((Error?) -> ())) {
        dataOpsSerializationQueue.async { [weak self] in
            if let selfReference = self {
                
                // Not technically a failure, just exit early in empty boundary case
                guard contentsOf.count > 0 else {
                    completion(nil)
                    return
                }
                
                guard PersistantConcurrentTimeSeries.isSorted(contentsOf) else {
                    completion(TimeSeriesError.nonAscendingOrderArgument)
                    return
                }

                guard selfReference.inMemoryData.first == nil ||
                    contentsOf.last!.timestamp <= selfReference.inMemoryData.first!.timestamp else {
                    completion(TimeSeriesError.nonAscendingOrder)
                    return
                }
                
                selfReference.inMemoryData = contentsOf + selfReference.inMemoryData
                Pantry.pack(selfReference.inMemoryData, key: selfReference.label)
                completion(nil)
            }
            
            else {
                completion(nil)
            }
        }
    }

    public func get() -> AnyRandomAccessCollection<T> {
        return AnyRandomAccessCollection(inMemoryData)
    }

    // Get all items since date, non-inclusive
    public func get(since: Date) -> AnyRandomAccessCollection<T> {
        
        // Trivial case - empty list or since is after latest timestamp
        if inMemoryData.last == nil || inMemoryData.last!.timestamp <= since {
            return AnyRandomAccessCollection([])
        }
        
        // Trivial case - since is before earliest timestamp
        // Algorithm note - this also handles edge case where have list of size 1
        if inMemoryData.first!.timestamp > since {
            return AnyRandomAccessCollection(inMemoryData)
        }
        
        let startIndex = binarySearch { timestamp -> Bool in
            return timestamp <= since
        }
        let slice = inMemoryData[startIndex..<inMemoryData.count]
        return AnyRandomAccessCollection(slice)
    }
    
    // Drop all items until date inclusive.
    // TODO - implement lazy drop - just mark what needs to be truncated and do so when
    //        writing back after next append
    public func drop(until: Date, completion: @escaping ((Error?) -> ())) {
        dataOpsSerializationQueue.async { [weak self] in
            if let selfReference = self {
                let newInMemoryData = Array(selfReference.get(since: until))
                selfReference.inMemoryData = newInMemoryData
                Pantry.pack(newInMemoryData, key: selfReference.label)
                completion(nil)
            }
            else {
                completion(nil)
            }
        }
    }
    
    // Adapted from https://stackoverflow.com/questions/31904396/swift-binary-search-for-standard-array
    //
    // Finds such index N that predicate is true for all elements up to
    // but not including the index N, and is false for all elements
    // starting with index N.
    // Behavior is undefined if there is no such N.
    private func binarySearch(predicate: (Date) -> Bool) -> Int {
        var low = 0
        var high = inMemoryData.count - 1
        while low != high {
            let mid = (high + low) / 2
            if predicate(inMemoryData[mid].timestamp) {
                low = mid + 1
            } else {
                high = mid
            }
        }
        return low
    }
    
    // Adapted from https://stackoverflow.com/questions/24602595/extending-array-to-check-if-it-is-sorted-in-swift
    static private func isSorted(_ contentsOf: Array<T>) -> Bool {
        for i in 1..<contentsOf.count {
            if contentsOf[i-1].timestamp > contentsOf[i].timestamp {
                return false
            }
        }
        return true
    }
}
