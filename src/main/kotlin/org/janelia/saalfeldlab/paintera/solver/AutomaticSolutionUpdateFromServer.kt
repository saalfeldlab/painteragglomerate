package org.janelia.saalfeldlab.paintera.solver

import gnu.trove.map.hash.TLongLongHashMap

interface AutomaticSolutionUpdateFromServer {

    fun latestSolution(): TLongLongHashMap

}
