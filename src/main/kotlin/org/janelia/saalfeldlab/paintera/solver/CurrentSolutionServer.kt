package org.janelia.saalfeldlab.paintera.solver

import gnu.trove.map.hash.TLongLongHashMap

interface CurrentSolutionServer {

    fun currentSolution() : TLongLongHashMap

    @Throws(UnableToUpdate::class)
    fun updateCurrentSolution()

    class UnableToUpdate : Exception()
    {

    }

}