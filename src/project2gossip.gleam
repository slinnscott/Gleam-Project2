import argv
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/otp/actor
import gleam/time/duration
import gleam/time/timestamp

pub type Actor {
  Actor(
    id: Int,
    neighbors: List(Int),
    rumor_count: Int,
    s: Float,
    w: Float,
    stable_rounds: Int,
    main_reply: process.Subject(List(Int)),
  )
}

pub fn cube_size(n: Int, size: Int) -> Int {
  case size * size * size >= n {
    True -> size
    False -> cube_size(n, size + 1)
  }
}

// Build a 3D grid for num_nodes nodes
pub fn build_3d(
  num_nodes: Int,
  main_reply: process.Subject(List(Int)),
) -> List(Actor) {
  let size = cube_size(num_nodes, 1)

  let ids = list.range(0, num_nodes - 1)

  list.map(ids, fn(id) {
    let assert Ok(x) = int.modulo(id, by: size)
    let y_div = id / size
    let assert Ok(y) = int.modulo(y_div, by: size)
    let z = id / { size * size }

    let neighbors: List(Int) = []

    let neighbors = case x > 0 {
      True -> {
        let left_neighbor = z * size * size + y * size + x - 1
        [left_neighbor, ..neighbors]
      }
      False -> neighbors
    }

    let neighbors = case x < size - 1 {
      True -> {
        let right_neighbor = z * size * size + y * size + x + 1
        [right_neighbor, ..neighbors]
      }
      False -> neighbors
    }

    let neighbors = case y > 0 {
      True -> {
        let y_coord = y - 1
        let up_neighbor = z * size * size + y_coord * size + x
        [up_neighbor, ..neighbors]
      }
      False -> neighbors
    }

    let neighbors = case y < size - 1 {
      True -> {
        let y_coord = y + 1
        let down_neighbor = z * size * size + y_coord * size + x
        [down_neighbor, ..neighbors]
      }
      False -> neighbors
    }

    let neighbors = case z > 0 {
      True -> {
        let z_coord = z - 1
        let back_neighbor = z_coord * size * size + y * size + x
        [back_neighbor, ..neighbors]
      }
      False -> neighbors
    }

    let neighbors = case z < size - 1 {
      True -> {
        let z_coord = z + 1
        let front_neighbor = z_coord * size * size + y * size + x
        [front_neighbor, ..neighbors]
      }
      False -> neighbors
    }

    // Remove any neighbors that exceed the requested node count -> Cube may be larger than total number of nodes
    let neighbors = list.filter(neighbors, fn(n) { n < num_nodes })
    Actor(id, neighbors, 0, int.to_float(id), 1.0, 0, main_reply)
  })
}

fn pick_extra_neighbor(id: Int, num_nodes: Int) -> Int {
  let seed = id * 1_664_525 + 1_013_904_223
  let assert Ok(r0) = int.modulo(seed, by: num_nodes)
  // If we somehow get the ourselves as an extra neighbor, we pick the next one
  case r0 == id {
    True -> {
      let assert Ok(r1) = int.modulo(r0 + 1, by: num_nodes)
      r1
    }
    False -> r0
  }
}

// Build imperfect 3D = Build 3D + one extra neighbor
pub fn build_imperfect_3d(
  num_nodes: Int,
  main_reply: process.Subject(List(Int)),
) -> List(Actor) {
  let base = build_3d(num_nodes, main_reply)
  list.map(base, fn(actor) {
    case actor {
      Actor(id, neighbors, rumor_count, s, w, stable_rounds, main_reply) -> {
        let extra = pick_extra_neighbor(id, num_nodes)
        let new_neighbors = [extra, ..neighbors]
        Actor(id, new_neighbors, rumor_count, s, w, stable_rounds, main_reply)
      }
    }
  })
}

// Build full network = every node is connected to every other node
pub fn build_full_network(
  num_nodes: Int,
  main_reply: process.Subject(List(Int)),
) -> List(Actor) {
  let ids = list.range(0, num_nodes - 1)
  list.map(ids, fn(id) {
    let neighbors = list.filter(ids, fn(n) { n != id })
    Actor(id, neighbors, 0, int.to_float(id), 1.0, 0, main_reply)
  })
}

// Build line network = nodes connected in a line
pub fn build_line(
  num_nodes: Int,
  main_reply: process.Subject(List(Int)),
) -> List(Actor) {
  let ids = list.range(0, num_nodes - 1)

  list.map(ids, fn(id) {
    let neighbors: List(Int) = []

    let neighbors = case id > 0 {
      True -> [id - 1, ..neighbors]
      False -> neighbors
    }

    let neighbors = case id < num_nodes - 1 {
      True -> [id + 1, ..neighbors]
      False -> neighbors
    }
    Actor(id, neighbors, 0, int.to_float(id), 1.0, 0, main_reply)
  })
}

pub type GossipMessage {
  Rumor(rumor_value: Float)
  RequestSum
  SumResponse(sum: Float, count: Int)
  StartGossip
  StopGossip
}

pub type PushSumMessage {
  PushSum(sum: Float, weight: Float)
  StartPushSum
  StopPushSum
}

pub type GossipState {
  GossipState(
    actor: Actor,
    all_actors: List(Actor),
    actor_subjects: List(process.Subject(GossipMessage)),
    convergence_threshold: Int,
    is_converged: Bool,
  )
}

pub type PushSumState {
  PushSumState(
    actor: Actor,
    all_actors: List(Actor),
    actor_subjects: List(process.Subject(PushSumMessage)),
    convergence_threshold: Int,
    is_converged: Bool,
  )
}

pub fn gossip_actor_handler(
  state: GossipState,
  message: GossipMessage,
) -> actor.Next(GossipState, GossipMessage) {
  case message {
    Rumor(rumor_value) -> {
      case state {
        GossipState(actor, all_actors, actor_subjects, threshold, converged) -> {
          // Update actor with new rumor
          let new_stable_rounds = case actor.s == rumor_value {
            True -> actor.stable_rounds + 1
            False -> 0
          }

          let updated_actor =
            Actor(
              actor.id,
              actor.neighbors,
              actor.rumor_count + 1,
              actor.s,
              actor.w,
              new_stable_rounds,
              actor.main_reply,
            )

          let new_state =
            GossipState(
              updated_actor,
              all_actors,
              actor_subjects,
              threshold,
              converged,
            )

          let is_converged = updated_actor.stable_rounds >= threshold

          case is_converged && !converged {
            True -> {
              // Send stop message to all actors
              list.each(actor_subjects, fn(subject) {
                process.send(subject, StopGossip)
              })

              actor.continue(GossipState(
                updated_actor,
                all_actors,
                actor_subjects,
                threshold,
                True,
              ))
            }
            False -> actor.continue(new_state)
          }
        }
      }
    }

    StartGossip -> {
      case state {
        GossipState(actor, _all_actors, actor_subjects, _threshold, converged) -> {
          case !converged {
            True -> {
              // Pick a random neighbor and send rumor
              case list.length(actor.neighbors) {
                0 -> actor.continue(state)
                _ -> {
                  let neighbor_index = actor.id % list.length(actor.neighbors)

                  case list.drop(actor.neighbors, neighbor_index) {
                    [neighbor_id, ..] -> {
                      case list.drop(actor_subjects, neighbor_id) {
                        [neighbor_subject, ..] -> {
                          process.send(neighbor_subject, Rumor(actor.s))
                          actor.continue(state)
                        }
                        [] -> actor.continue(state)
                      }
                    }
                    [] -> actor.continue(state)
                  }
                }
              }
            }
            False -> {
              process.send(actor.main_reply, [])
              actor.continue(state)
            }
          }
        }
      }
    }

    StopGossip -> {
      case state {
        GossipState(actor, all_actors, actor_subjects, threshold, _converged) -> {
          actor.continue(GossipState(
            actor,
            all_actors,
            actor_subjects,
            threshold,
            True,
          ))
        }
      }
    }

    RequestSum -> {
      case state {
        GossipState(
          _actor,
          _all_actors,
          _actor_subjects,
          _threshold,
          _converged,
        ) -> {
          actor.continue(state)
        }
      }
    }

    SumResponse(_sum, _count) -> {
      actor.continue(state)
    }
  }
}

// Push Sum actor handler
pub fn pushsum_actor_handler(
  state: PushSumState,
  message: PushSumMessage,
) -> actor.Next(PushSumState, PushSumMessage) {
  case message {
    PushSum(sum, weight) -> {
      case state {
        PushSumState(actor, all_actors, actor_subjects, threshold, converged) -> {
          let new_s = actor.s +. sum
          let new_w = actor.w +. weight

          // Check if the ratio has stabilized
          let old_ratio = actor.s /. actor.w
          let new_ratio = new_s /. new_w
          let ratio_change = old_ratio -. new_ratio
          let is_stable =
            ratio_change <. 0.0000001 && ratio_change >. -0.0000001

          let new_stable_rounds = case is_stable {
            True -> actor.stable_rounds + 1
            False -> 0
          }

          let updated_actor =
            Actor(
              actor.id,
              actor.neighbors,
              actor.rumor_count + 1,
              new_s,
              new_w,
              new_stable_rounds,
              actor.main_reply,
            )

          let new_state =
            PushSumState(
              updated_actor,
              all_actors,
              actor_subjects,
              threshold,
              converged,
            )

          // Check for convergence
          let is_converged = updated_actor.stable_rounds >= threshold

          case is_converged && !converged {
            True -> {
              list.each(actor_subjects, fn(subject) {
                process.send(subject, StopPushSum)
              })

              actor.continue(PushSumState(
                updated_actor,
                all_actors,
                actor_subjects,
                threshold,
                True,
              ))
            }
            False -> actor.continue(new_state)
          }
        }
      }
    }

    StartPushSum -> {
      case state {
        PushSumState(actor, all_actors, actor_subjects, threshold, converged) -> {
          case !converged {
            True -> {
              // Pick a random neighbor and send half of our sum and weight
              case list.length(actor.neighbors) {
                0 -> actor.continue(state)
                _ -> {
                  let neighbor_index = actor.id % list.length(actor.neighbors)

                  case list.drop(actor.neighbors, neighbor_index) {
                    [neighbor_id, ..] -> {
                      case list.drop(actor_subjects, neighbor_id) {
                        [neighbor_subject, ..] -> {
                          let half_sum = actor.s /. 2.0
                          let half_weight = actor.w /. 2.0

                          let updated_actor =
                            Actor(
                              actor.id,
                              actor.neighbors,
                              actor.rumor_count,
                              half_sum,
                              half_weight,
                              actor.stable_rounds,
                              actor.main_reply,
                            )

                          process.send(
                            neighbor_subject,
                            PushSum(half_sum, half_weight),
                          )
                          actor.continue(PushSumState(
                            updated_actor,
                            all_actors,
                            actor_subjects,
                            threshold,
                            converged,
                          ))
                        }
                        [] -> actor.continue(state)
                      }
                    }
                    [] -> actor.continue(state)
                  }
                }
              }
            }
            False -> actor.continue(state)
          }
        }
      }
    }

    StopPushSum -> {
      case state {
        PushSumState(actor, all_actors, actor_subjects, threshold, _converged) -> {
          actor.continue(PushSumState(
            actor,
            all_actors,
            actor_subjects,
            threshold,
            True,
          ))
        }
      }
    }
  }
}

fn parse_cmdline_args(
  args: List(String),
) -> Result(#(Int, String, String), String) {
  case args {
    [num_nodes_str, topology, algorithm] -> {
      case int.parse(num_nodes_str) {
        Ok(num_nodes) -> {
          case num_nodes <= 0 {
            True -> Error("Number of nodes must be positive")
            False -> {
              case algorithm {
                "gossip" -> Ok(#(num_nodes, topology, algorithm))
                "push-sum" -> Ok(#(num_nodes, topology, algorithm))
                _ -> Error("Unknown algorithm. Use: gossip or push-sum")
              }
            }
          }
        }
        Error(_) -> Error("Invalid number of nodes")
      }
    }
    _ ->
      Error(
        "Usage: gleam run <num_nodes> <topology> <algorithm>\nTopologies: full, line, 3d, imperfect3d\nAlgorithms: gossip, push-sum",
      )
  }
}

// Create actors based on topology
fn create_actors(
  num_nodes: Int,
  topology: String,
  main_reply: process.Subject(List(Int)),
) -> Result(List(Actor), String) {
  case topology {
    "full" -> Ok(build_full_network(num_nodes, main_reply))
    "line" -> Ok(build_line(num_nodes, main_reply))
    "3d" -> Ok(build_3d(num_nodes, main_reply))
    "imperfect3d" -> Ok(build_imperfect_3d(num_nodes, main_reply))
    _ -> Error("Unknown topology. Use: full, line, 3d, or imperfect3d")
  }
}

fn start_gossip_actors(
  actors: List(Actor),
  convergence_threshold: Int,
) -> Result(List(process.Subject(GossipMessage)), String) {
  // First, create all actors to get their subjects
  let actor_results =
    list.map(actors, fn(actor) {
      let initial_state =
        GossipState(actor, actors, [], convergence_threshold, False)

      actor.new(initial_state)
      |> actor.on_message(gossip_actor_handler)
      |> actor.start
    })

  let actor_subjects =
    list.filter_map(actor_results, fn(result) {
      case result {
        Ok(actor.Started(pid: _, data: subject)) -> Ok(subject)
        Error(_) -> Error(Nil)
      }
    })

  Ok(actor_subjects)
}

fn start_pushsum_actors(
  actors: List(Actor),
  convergence_threshold: Int,
) -> Result(List(process.Subject(PushSumMessage)), String) {
  // First, create all actors to get their subjects
  let actor_results =
    list.map(actors, fn(actor) {
      let initial_state =
        PushSumState(actor, actors, [], convergence_threshold, False)

      actor.new(initial_state)
      |> actor.on_message(pushsum_actor_handler)
      |> actor.start
    })

  let actor_subjects =
    list.filter_map(actor_results, fn(result) {
      case result {
        Ok(actor.Started(pid: _, data: subject)) -> Ok(subject)
        Error(_) -> Error(Nil)
      }
    })

  Ok(actor_subjects)
}

fn run_gossip_simulation(
  _actors: List(Actor),
  actor_subjects: List(process.Subject(GossipMessage)),
  _convergence_threshold: Int,
  main_reply: process.Subject(List(Int)),
) -> Nil {
  let start_time = timestamp.system_time()
  list.each(actor_subjects, fn(subject) { process.send(subject, StartGossip) })
  process.receive_forever(main_reply)
  let end_time = timestamp.system_time()
  let duration = timestamp.difference(start_time, end_time)
  io.println(
    "Total time taken: "
    <> float.to_string(duration.to_seconds(duration))
    <> " seconds",
  )
  io.println("Gossip simulation completed")
}

fn run_pushsum_simulation(
  _actors: List(Actor),
  actor_subjects: List(process.Subject(PushSumMessage)),
  _convergence_threshold: Int,
  main_reply: process.Subject(List(Int)),
) -> Nil {
  let start_time = timestamp.system_time()
  list.each(actor_subjects, fn(subject) { process.send(subject, StartPushSum) })
  process.receive_forever(main_reply)
  let end_time = timestamp.system_time()
  let duration = timestamp.difference(start_time, end_time)
  io.println(
    "Total time taken: "
    <> float.to_string(duration.to_seconds(duration))
    <> " seconds",
  )
  io.println("Push Sum simulation completed")
}

// Main function
pub fn main() -> Nil {
  let args = argv.load().arguments

  case parse_cmdline_args(args) {
    Ok(#(num_nodes, topology, algorithm)) -> {
      let main_reply = process.new_subject()
      case create_actors(num_nodes, topology, main_reply) {
        Ok(actors) -> {
          let convergence_threshold = 10

          case algorithm {
            "gossip" -> {
              case start_gossip_actors(actors, convergence_threshold) {
                Ok(actor_subjects) -> {
                  io.println("Starting gossip simulation...")
                  io.println("Number of nodes: " <> int.to_string(num_nodes))
                  io.println("Topology: " <> topology)
                  io.println("Algorithm: " <> algorithm)

                  run_gossip_simulation(
                    actors,
                    actor_subjects,
                    convergence_threshold,
                    main_reply,
                  )
                }
                Error(msg) ->
                  io.println("Error starting gossip actors: " <> msg)
              }
            }
            "push-sum" -> {
              case start_pushsum_actors(actors, convergence_threshold) {
                Ok(actor_subjects) -> {
                  io.println("Starting Push Sum simulation...")
                  io.println("Number of nodes: " <> int.to_string(num_nodes))
                  io.println("Topology: " <> topology)
                  io.println("Algorithm: " <> algorithm)

                  run_pushsum_simulation(
                    actors,
                    actor_subjects,
                    convergence_threshold,
                    main_reply,
                  )
                }
                Error(msg) ->
                  io.println("Error starting Push Sum actors: " <> msg)
              }
            }
            _ -> io.println("Unknown algorithm: " <> algorithm)
          }
        }
        Error(msg) -> io.println("Error creating actors: " <> msg)
      }
    }
    Error(msg) -> io.println("Error: " <> msg)
  }
}
