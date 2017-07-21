// Copyright © 2017 Quantac. All rights reserved.

import Foundation

// Any item that has a time series index
public protocol TimeSeriesItem {
    var timestamp : Date {get}
}
