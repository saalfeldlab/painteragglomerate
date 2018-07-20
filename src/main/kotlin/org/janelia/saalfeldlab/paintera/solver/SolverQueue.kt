package org.janelia.saalfeldlab.paintera.solver

import gnu.trove.map.hash.TLongLongHashMap
import org.janelia.saalfeldlab.paintera.control.assignment.action.AssignmentAction
import org.janelia.saalfeldlab.util.NamedThreadFactory
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer
import java.util.function.Supplier

class SolverQueue(
        actionReceiver: Supplier<Iterable<AssignmentAction>>,
        actionReceiptConfirmation: Runnable,
        solutionRequestToSolver: Consumer<Collection<AssignmentAction>>,
        solutionReceiver: Supplier<TLongLongHashMap>,
        solutionDistributor: Consumer<TLongLongHashMap>,
        initialSolution: Supplier<TLongLongHashMap>,
        currentSolutionRequest: Supplier<Void>,
        currentSolutionResponse: Consumer<TLongLongHashMap>,
        minWaitTimeAfterLastAction: Long) {

    companion object {
        private val LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

        private val SCHEDULING_PERIOD : Long = 10

        private val SCHEDULING_PERIOD_UNIT = TimeUnit.MILLISECONDS
    }

    private val queue = ArrayList<AssignmentAction>()

    private val timeOfLastAction = AtomicLong(0)

    private val actionReceiverService: ScheduledExecutorService = Executors.newScheduledThreadPool(1, NamedThreadFactory("${javaClass.simpleName}-receive-action", false))

    private val solutionHandlerService: ScheduledExecutorService = Executors.newScheduledThreadPool(1, NamedThreadFactory("${javaClass.simpleName}-handle-solution", false))

    private val currentSolutionProviderSerivce: ScheduledExecutorService = Executors.newScheduledThreadPool(1, NamedThreadFactory("${javaClass.simpleName}-current-solution-provider", false))

    private val latestSolution: TLongLongHashMap

    class Solution

    init {

        this.latestSolution = initialSolution.get()

        actionReceiverService.scheduleAtFixedRate(
            {
                LOG.debug("Waiting for action in queue!")
                val actions = actionReceiver.get()
                LOG.debug("Got action in queue! " + actions)
                if (actions != null)
                    synchronized(queue) {
                        timeOfLastAction.set(System.currentTimeMillis())
                        actions.forEach(Consumer<AssignmentAction> { queue.add(it) })
                        actionReceiptConfirmation.run()
                    }
               },
                0,
                SCHEDULING_PERIOD,
                SCHEDULING_PERIOD_UNIT
            )

        solutionHandlerService.scheduleAtFixedRate({
                var sentRequest = false
                synchronized(this.queue) {
                    val currentTime = System.currentTimeMillis()
                    val timeDiff = currentTime - timeOfLastAction.get()

                    if (timeDiff >= minWaitTimeAfterLastAction)

                        if (queue.size > 0) {
                            solutionRequestToSolver.accept(queue)
                            sentRequest = sentRequest or true
                            queue.clear()
                        }


                    if (sentRequest) {
                        val solution = solutionReceiver.get()
                        synchronized(latestSolution) {
                            this.latestSolution.clear()
                            this.latestSolution.putAll(solution)
                            solutionDistributor.accept(latestSolution)
                        }
                    }
                }
        }, 0, 10, TimeUnit.MILLISECONDS )


        currentSolutionProviderSerivce.scheduleAtFixedRate({
            val empty = currentSolutionRequest.get()
            val currentSolution : TLongLongHashMap = getCurrentSolution()
            currentSolutionResponse.accept(currentSolution)
        },
                0,
                SCHEDULING_PERIOD,
                SCHEDULING_PERIOD_UNIT )

    }

    fun getCurrentSolution() : TLongLongHashMap
    {
        synchronized(this.latestSolution)
        {
            return TLongLongHashMap(this.latestSolution)
        }
    }

    fun forceStop()
    {
        actionReceiverService.shutdownNow()
        solutionHandlerService.shutdownNow()
    }

}
