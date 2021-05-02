package edu.upenn.cis.stormlite;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.upenn.cis.stormlite.tasks.ITask;

public class FairTaskQueue {
    static Logger log = LogManager.getLogger(FairTaskQueue.class);

    Queue<Queue<ITask>> fairTaskQueue = new ConcurrentLinkedQueue<Queue<ITask>>();
    Map<String, Queue<ITask>> fairTaskMap = new ConcurrentHashMap<String, Queue<ITask>>();

    public synchronized Queue<ITask> nextQueue() {
        Queue<ITask> taskQueue = fairTaskQueue.poll();

        if (taskQueue != null) {
            // Make sure to always add queue back to the end.
            fairTaskQueue.add(taskQueue);
        }
        return taskQueue;
    }

    public synchronized Queue<ITask> getQueue(String className) {
        return fairTaskMap.get(className);
    }

    public synchronized void addClass(String className) {
        // log.info("Adding class: " + className);
        Queue<ITask> taskQueue = new ConcurrentLinkedQueue<ITask>();
        fairTaskMap.put(className, taskQueue);
        fairTaskQueue.add(taskQueue);
    }

    public synchronized void addTask(String className, ITask task) {
        // log.info("Adding task to " + className);
        fairTaskMap.get(className).add(task);
    }

    public synchronized String toString() {
        String res = "FAIR TASK QUEUE:[";
        int size = 0;
        for (String key : fairTaskMap.keySet()) {
            res += key + "=" + fairTaskMap.get(key).size() + ",";
            size += fairTaskMap.get(key).size();
        }
        res = res.substring(0, res.length() - 1) + "]\t";
        res += "size=" + size;
        return res;
    }

    public synchronized String getKey(Queue<ITask> taskQueue) {
        for (String key : fairTaskMap.keySet()) {
            if (taskQueue == fairTaskMap.get(key)) {
                return key;
            }
        }
        return "";
    }
}
