// Copyright © 2017 Quantac. All rights reserved.

import Foundation

// Error types specific to usage with this class
public enum TimeSeriesError: Error {
    
    // A put operation (prepend or append) adds items
    // not in order against the existing collection
    // stored in the time series.
    case nonAscendingOrder
    
    // A put operation's argument is invalid as the
    // collection is not in order (currently this class
    // is always paranoid and checks every time)
    case nonAscendingOrderArgument
}
